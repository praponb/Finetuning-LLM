from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        F.explode_outer("context").alias("attr_key", "value"),
    )
)

result = (
    lines
    .select("id", "service", "level", "latency_ms", "attr_key", "value")
    .orderBy(F.col("level").asc_nulls_first())
)
