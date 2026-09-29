from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host")

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "value"),
    )
)

result = (
    flattened
    .select(
        "id",
        "host",
        F.corr("status_code", "latency_ms").over(w).alias("corr_status_code_latency_ms"),
    )
)
