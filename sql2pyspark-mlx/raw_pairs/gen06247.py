from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "attr_value"),
    )
)

result = (
    exploded
    .select("id", "service", "level", "latency_ms", "k", "attr_value")
    .orderBy(F.col("level").asc_nulls_first(), F.col("latency_ms").desc())
)
