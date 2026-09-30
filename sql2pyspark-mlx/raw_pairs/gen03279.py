from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("key", "v"),
    )
)

result = (
    lines
    .select("id", "host", "status_code", "key", "v")
    .orderBy(F.col("status_code").desc_nulls_last())
)
