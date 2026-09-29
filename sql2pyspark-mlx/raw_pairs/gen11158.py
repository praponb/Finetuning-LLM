from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host", "service")
w_grp = Window.partitionBy("service")
w3 = Window.partitionBy("key", "host")
w4 = Window.orderBy(F.col("status_code").desc())

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("key", "attr_value"),
    )
)

result = (
    flattened
    .select(
        "id",
        "host",
        "status_code",
        "key",
        "attr_value",
        F.corr("status_code", "latency_ms").over(w).alias("corr_status_code_latency_ms"),
        F.max("latency_ms").over(w_grp).alias("service_max_latency_ms"),
        F.min("latency_ms").over(w3).alias("key_min_latency_ms"),
        F.round((F.col("latency_ms") / F.sum("latency_ms").over(w_grp)), 4).alias("latency_ms_share"),
        F.dense_rank().over(w4).alias("status_code_dense_rank"),
    )
)
