from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "attr_value"),
    )
)

result = (
    exploded
    .select(
        "service",
        "latency_ms",
        "status_code",
        F.when(F.col("service").rlike("^(billing|auth)$"), "OTHER_ZONE").otherwise("CORE").alias("service_zone"),
    )
    .orderBy(F.col("service_zone").desc())
)
