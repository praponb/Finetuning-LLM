from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        F.explode_outer("context").alias("key", "value"),
    )
)

result = (
    flattened
    .select("service", "level", "latency_ms", "value")
    .orderBy(F.col("service").desc_nulls_last())
)
