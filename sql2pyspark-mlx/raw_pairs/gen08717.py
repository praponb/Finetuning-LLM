from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy(F.col("latency_ms").desc_nulls_first())
w2 = Window.partitionBy("level")
w3 = Window.partitionBy("attr_key")
w_running = Window.partitionBy("level", "attr_key")

lines = (
    df_app_logs
    .select(
        "id",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "value"),
    )
)

result = (
    lines
    .filter(F.col("latency_ms") == 2083)
    .select(
        "id",
        "level",
        "latency_ms",
        "status_code",
        "attr_key",
        "value",
        F.cume_dist().over(w).alias("latency_ms_cume_dist"),
        F.round((F.col("latency_ms") / F.sum("latency_ms").over(w2)), 4).alias("latency_ms_share"),
        F.corr("latency_ms", "status_code").over(w3).alias("corr_latency_ms_status_code"),
        F.corr("latency_ms", "status_code").over(w_running).alias("corr_latency_ms_status_code_2"),
    )
)
