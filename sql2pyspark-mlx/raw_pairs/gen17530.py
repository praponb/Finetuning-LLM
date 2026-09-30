from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        F.explode_outer("context").alias("key", "attr_value"),
    )
)

result = (
    flattened
    .select("id", "service", "host", "level", "latency_ms", "key")
    .orderBy(F.col("id").desc())
)
