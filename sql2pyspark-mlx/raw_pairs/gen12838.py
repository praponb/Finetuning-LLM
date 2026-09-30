from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode("context").alias("key", "value"),
    )
)

result = (
    exploded
    .select(
        "id",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        "key",
        F.when(F.col("key").rlike("^(trace|region)$"), "PRIORITY").when(F.col("key").rlike("^(version)$"), "EXTENDED").otherwise("OTHER_ZONE").alias("key_zone"),
    )
    .orderBy("host")
)
