from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        F.explode_outer("context").alias("attr_key", "attr_value"),
    )
)

result = (
    lines
    .select(
        "id",
        "service",
        "level",
        "attr_value",
        F.when(F.col("service").rlike("^(auth|search)$"), "CORE").otherwise("OTHER_ZONE").alias("service_zone"),
    )
    .orderBy("id")
)
