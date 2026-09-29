from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("attr_key", "value"),
    )
)

result = (
    exploded
    .select("id", "service", "level", "status_code", "attr_key", "value")
    .orderBy(F.col("id").asc_nulls_last())
)
