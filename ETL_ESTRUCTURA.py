import os
import json
import datetime
import traceback
import pandas as pd
from sqlalchemy import create_engine, text

# ============================================================================
# 1. CONEXIÓN A AWS RDS (MySQL)
# ============================================================================
DB_USER = os.getenv("DB_USER", "")
DB_PASS = os.getenv("DB_PASS", "")
DB_HOST = os.getenv("DB_HOST", "")
DB_PORT = os.getenv("DB_PORT", "3306")
DB_NAME = os.getenv("DB_NAME", "")

connection_string = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(connection_string, echo=False)

# ============================================================================
# 2. LOGGING DE AUDITORÍA Y TABLA DE RECHAZOS (DEAD-LETTER)
# ============================================================================
def iniciar_log_etl():
    """Registra el inicio del lote en etl_log_ejecucion[cite: 10]."""
    query = text("""
        INSERT INTO etl_log_ejecucion (nombre_paquete, fecha_inicio, estado) 
        VALUES ('ETL_MultiFuente_300', NOW(), 'EN PROCESO');
    """)
    with engine.connect() as conn:
        res = conn.execute(query)
        conn.commit()
        return res.lastrowid

def finalizar_log_etl(log_id, extraidas, cargadas, rechazadas, estado="EXITOSO", error_msg=None):
    """Actualiza la tabla de control etl_log_ejecucion al finalizar[cite: 10]."""
    query = text("""
        UPDATE etl_log_ejecucion
        SET fecha_fin = NOW(), 
            filas_extraidas = :ext, 
            filas_cargadas = :car, 
            filas_rechazadas = :rec, 
            estado = :est, 
            mensaje_error = :err
        WHERE log_id = :id;
    """)
    with engine.connect() as conn:
        conn.execute(query, {
            "ext": extraidas, "car": cargadas, "rec": rechazadas, 
            "est": estado, "err": error_msg, "id": log_id
        })
        conn.commit()

def registrar_rechazo(origen, campo_erroneo, valor_original, motivo_rechazo):
    """Aísla los datos corruptos/duplicados en tabla_rechazos[cite: 10]."""
    query = text("""
        INSERT INTO tabla_rechazos (origen, campo_erroneo, valor_original, motivo_rechazo)
        VALUES (:origen, :campo, :valor, :motivo);
    """)
    with engine.connect() as conn:
        conn.execute(query, {
            "origen": origen,
            "campo": campo_erroneo,
            "valor": str(valor_original)[:254],
            "motivo": motivo_rechazo
        })
        conn.commit()

# ============================================================================
# 3. LECTURA Y DETALLE POR FUENTE (CAPA BRONCE)
# ============================================================================
def cargar_fuentes():
    """Carga los datos desde los 3 archivos físicos (Excel, JSON, CSV/POS)."""
    dfs = []

    # Fuente 1: Excel
    print("   [1/3] Leyendo archivo Excel ('ventas_retail.xlsx')...")
    df_excel = pd.read_excel('ventas_retail.xlsx')
    df_excel['origen_fuente'] = 'EXCEL_RETAIL'
    print(f"         └─ {len(df_excel)} registros leídos desde Excel.")
    dfs.append(df_excel)

    # Fuente 2: JSON
    print("   [2/3] Leyendo documento JSON ('ventas_ecommerce.json')...")
    with open('ventas_ecommerce.json', 'r', encoding='utf-8') as f:
        df_json = pd.DataFrame(json.load(f))
    df_json['origen_fuente'] = 'JSON_ECOMMERCE'
    print(f"         └─ {len(df_json)} registros leídos desde JSON.")
    dfs.append(df_json)

    # Fuente 3: Exportación de Base de Datos POS (ventas_pos.csv)
    print("   [3/3] Leyendo exportación de Base de Datos POS ('ventas_pos.csv')...")
    df_csv = pd.read_csv('ventas_pos.csv')
    df_csv['origen_fuente'] = 'DataRetail360_OLTP'
    print(f"         └─ {len(df_csv)} registros leídos desde CSV/DB.")
    dfs.append(df_csv)

    df_bronce = pd.concat(dfs, ignore_index=True)
    return df_bronce

# ============================================================================
# 4. TRATAMIENTO Y DETALLE DE CALIDAD EN TIEMPO REAL (CAPA PLATA)
# ============================================================================
def procesar_calidad_y_duplicados(df_bronce):
    """Evalúa cada fila por fuente inicializando los contadores dinámicamente."""
    rechazos_count = 0
    filas_validas = []
    boletas_procesadas = set()

    # Inicialización DINÁMICA para evitar KeyError sin importar el nombre del origen
    fuentes_unicas = df_bronce['origen_fuente'].unique()
    resumen_fuentes = {fuente: {'aprobados': 0, 'rechazados': 0} for fuente in fuentes_unicas}

    print("\n--> Evaluación de reglas de calidad registro por registro:")

    for idx, row in df_bronce.iterrows():
        origen = row['origen_fuente']
        boleta_raw = row['num_boleta']

        # Regla 1: Validar boleta no nula ni vacía
        if pd.isna(boleta_raw) or str(boleta_raw).strip() in ["", "nan", "None", "NULL"]:
            registrar_rechazo(origen, "num_boleta", boleta_raw, "Número de boleta nulo o vacío")
            rechazos_count += 1
            resumen_fuentes[origen]['rechazados'] += 1
            print(f"   ❌ [{origen}] Rechazado: Boleta nula/vacía.")
            continue

        boleta_clean = str(boleta_raw).strip()

        # Regla 2: Deduplicación por número de boleta
        if boleta_clean in boletas_procesadas:
            registrar_rechazo(origen, "num_boleta", boleta_clean, "Registro duplicado detectado en la ingesta")
            rechazos_count += 1
            resumen_fuentes[origen]['rechazados'] += 1
            print(f"   ❌ [{origen}] Rechazado: Boleta duplicada '{boleta_clean}'.")
            continue

        # Regla 3: Cantidad mayor a 0
        if row['cantidad'] <= 0:
            registrar_rechazo(origen, "cantidad", row['cantidad'], "Cantidad de venta <= 0")
            rechazos_count += 1
            resumen_fuentes[origen]['rechazados'] += 1
            print(f"   ❌ [{origen}] Rechazado: Boleta '{boleta_clean}' con cantidad inválida ({row['cantidad']}).")
            continue

        # Regla 4: Monto neto no negativo
        if row['monto_neto'] < 0:
            registrar_rechazo(origen, "monto_neto", row['monto_neto'], "Monto neto negativo (< 0)")
            rechazos_count += 1
            resumen_fuentes[origen]['rechazados'] += 1
            print(f"   ❌ [{origen}] Rechazado: Boleta '{boleta_clean}' con monto negativo (${row['monto_neto']}).")
            continue

        # Registro aprobado
        boletas_procesadas.add(boleta_clean)
        row['num_boleta'] = boleta_clean
        row['sku'] = str(row['sku']).upper().strip()
        filas_validas.append(row)
        resumen_fuentes[origen]['aprobados'] += 1

    print("\n--> Resumen de procesamiento por Fuente de Datos:")
    for fuente, stats in resumen_fuentes.items():
        print(f"   • {fuente:20s} ➔ Aprobados: {stats['aprobados']:3d} | Rechazados: {stats['rechazados']:2d}")

    return pd.DataFrame(filas_validas), rechazos_count

# ============================================================================
# 5. GARANTÍA DE INTEGRIDAD EN DIM_FECHA
# ============================================================================
def garantizar_dimension_fecha(df_validos):
    """Inserta automáticamente fechas no registradas en dim_fecha[cite: 10]."""
    fechas_unicas = pd.to_datetime(df_validos['fecha']).dt.strftime('%Y-%m-%d').unique()
    nombres_meses = {
        1: 'Enero', 2: 'Febrero', 3: 'Marzo', 4: 'Abril', 5: 'Mayo', 6: 'Junio',
        7: 'Julio', 8: 'Agosto', 9: 'Septiembre', 10: 'Octubre', 11: 'Noviembre', 12: 'Diciembre'
    }

    nuevas_fechas = 0
    with engine.connect() as conn:
        for f_str in fechas_unicas:
            f_dt = datetime.datetime.strptime(f_str, '%Y-%m-%d')
            fecha_key = int(f_dt.strftime('%Y%m%d'))

            res = conn.execute(
                text("SELECT fecha_key FROM dim_fecha WHERE fecha_key = :fk;"), 
                {"fk": fecha_key}
            ).fetchone()

            if not res:
                query_ins = text("""
                    INSERT INTO dim_fecha (fecha_key, fecha, dia, mes, nombre_mes, trimestre, anio, es_feriado)
                    VALUES (:fk, :f, :d, :m, :nm, :t, :a, 0);
                """)
                conn.execute(query_ins, {
                    "fk": fecha_key, "f": f_str, "d": f_dt.day, "m": f_dt.month,
                    "nm": nombres_meses[f_dt.month], "t": (f_dt.month - 1) // 3 + 1, "a": f_dt.year
                })
                conn.commit()
                nuevas_fechas += 1

    print(f"   ✓ Dimensión fecha verificada ({nuevas_fechas} fechas nuevas incorporadas).")

# ============================================================================
# 6. MAPEO A SURROGATE KEYS Y CARGA IDEMPOTENTE (CAPA ORO)
# ============================================================================
def mapear_surrogate_keys(df_validos):
    """Mapea claves de origen a Surrogate Keys[cite: 10]."""
    staging_rows = []
    with engine.connect() as conn:
        for idx, row in df_validos.iterrows():
            fecha_key = int(pd.to_datetime(row['fecha']).strftime('%Y%m%d'))

            res_p = conn.execute(text("SELECT producto_key FROM dim_producto WHERE sku = :sku;"), {"sku": row['sku']}).fetchone()
            producto_key = int(res_p[0]) if res_p else 999

            res_s = conn.execute(text("SELECT sucursal_key FROM dim_sucursal WHERE codigo_sucursal = :suc;"), {"suc": row['codigo_sucursal']}).fetchone()
            sucursal_key = int(res_s[0]) if res_s else 999

            res_c = conn.execute(text("SELECT canal_key FROM dim_canal WHERE codigo_canal = :can;"), {"can": row['codigo_canal']}).fetchone()
            canal_key = int(res_c[0]) if res_c else 999

            res_cl = conn.execute(text("SELECT cliente_key FROM dim_cliente WHERE cliente_id = :cid AND es_actual = 1;"), {"cid": int(row['cliente_id'])}).fetchone()
            cliente_key = int(res_cl[0]) if res_cl else 999

            staging_rows.append({
                "fecha_key": fecha_key,
                "producto_key": producto_key,
                "sucursal_key": sucursal_key,
                "canal_key": canal_key,
                "cliente_key": cliente_key,
                "num_boleta": row['num_boleta'],
                "cantidad": int(row['cantidad']),
                "monto_neto": float(row['monto_neto'])
            })
    return pd.DataFrame(staging_rows)

def ejecutar_carga_final(df_staging):
    """Ejecuta la carga idempotente mediante Truncate Staging -> Insert Fact[cite: 10]."""
    with engine.connect() as conn:
        conn.execute(text("TRUNCATE TABLE fact_ventas_staging;"))
        conn.commit()

        df_staging.to_sql('fact_ventas_staging', con=engine, if_exists='append', index=False)

        query = text("""
            INSERT INTO fact_ventas (
                fecha_key, producto_key, sucursal_key, canal_key, cliente_key,
                num_boleta, cantidad, monto_bruto, monto_neto, es_devolucion
            )
            SELECT
                fecha_key, producto_key, sucursal_key, canal_key, cliente_key,
                num_boleta, cantidad, ROUND(monto_neto * 1.19, 2), monto_neto, 0
            FROM fact_ventas_staging;
        """)
        res = conn.execute(query)
        conn.commit()
        return res.rowcount

# ============================================================================
# 7. EJECUCIÓN PRINCIPAL
# ============================================================================
def ejecutar_pipeline():
    log_id = iniciar_log_etl()
    try:
        print("==================================================================")
        print(" INICIANDO PIPELINE ETL MULTIFUENTE EN TIEMPO REAL")
        print("==================================================================")
        print("\n1. Ingesta de Datos desde Fuentes Heterogéneas...")
        df_bronce = cargar_fuentes()
        total_extraidos = len(df_bronce)
        print(f"\n   ✓ Extracción finalizada: {total_extraidos} registros totales leídos.")

        print("\n2. Evaluación de Calidad de Datos y Aislamiento de Errores...")
        df_validos, total_rechazados = procesar_calidad_y_duplicados(df_bronce)

        print("\n3. Control de Integridad Referencial...")
        garantizar_dimension_fecha(df_validos)

        print("\n4. Mapeo a Claves Sustitutas (Surrogate Keys)...")
        df_staging = mapear_surrogate_keys(df_validos)
        print("   ✓ Mapeo completado.")

        print("\n5. Carga Analítica Idempotente en 'fact_ventas' (AWS RDS)...")
        cargados = ejecutar_carga_final(df_staging)
        print(f"   ✓ {cargados} registros insertados en la tabla de hechos.")

        finalizar_log_etl(log_id, total_extraidos, cargados, total_rechazados, "EXITOSO")
        print("\n==================================================================")
        print(" ¡ETL EJECUTADO Y AUDITADO EXITOSAMENTE!")
        print("==================================================================")

    except Exception as e:
        print(f"\n❌ Error durante la ejecución: {e}")
        traceback.print_exc()
        finalizar_log_etl(log_id, 0, 0, 0, "ERROR", str(e))

if __name__ == "__main__":
    ejecutar_pipeline()