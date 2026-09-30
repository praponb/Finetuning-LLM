from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("k", "v"),
    )
)

result = (
    flattened
    .select("id", "host", "status_code", "k", "v")
    .orderBy(F.col("k").desc_nulls_last())
)
