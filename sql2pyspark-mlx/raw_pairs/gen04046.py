from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("host", "attr_key").orderBy("latency_ms", "id", "attr_key")

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "latency_ms",
        "status_code",
        F.explode("context").alias("attr_key", "value"),
    )
)

result = (
    flattened
    .filter(F.col("latency_ms") <= 1485)
    .select(
        "id",
        "service",
        "host",
        "latency_ms",
        "attr_key",
        "value",
        F.lead("status_code").over(w_recent).alias("next_status_code"),
        F.lag("status_code").over(w_recent).alias("prev_status_code"),
        F.lead("latency_ms").over(w_recent).alias("next_latency_ms"),
        F.lag("latency_ms").over(w_recent).alias("prev_latency_ms"),
        F.row_number().over(w_recent).alias("seq_no"),
    )
)
