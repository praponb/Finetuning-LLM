from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "latency_ms",
        F.explode("context").alias("key", "v"),
    )
)

result = (
    lines
    .select(
        "id",
        "service",
        "latency_ms",
        "key",
        "v",
        F.when(F.col("service").rlike("^(auth|checkout)$"), "OTHER_ZONE").otherwise("GROUP_B").alias("service_zone"),
    )
    .orderBy(F.col("service_zone").asc_nulls_last())
)
