from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key", "host").orderBy("status_code", "id", "key")

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode("context").alias("key", "value"),
    )
)

result = (
    flattened
    .filter(F.col("key") == "version")
    .select(
        "id",
        "host",
        "log_ts",
        "latency_ms",
        "key",
        F.lag("latency_ms").over(w).alias("prev_latency_ms"),
        F.lead("status_code").over(w).alias("next_status_code"),
        F.row_number().over(w).alias("event_seq"),
    )
)
