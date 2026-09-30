from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k")

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "attr_value"),
    )
)

result = (
    exploded
    .select(
        "id",
        "service",
        "host",
        "latency_ms",
        "attr_value",
        F.corr("status_code", "latency_ms").over(w).alias("corr_status_code_latency_ms"),
    )
)
