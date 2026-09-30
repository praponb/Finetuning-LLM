from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("key", "attr_value"),
    )
)

result = (
    flattened
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "status_code",
        "key",
        F.when(F.col("level").rlike("^(WARN|DEBUG)$"), "GROUP_B").otherwise("OTHER_ZONE").alias("level_zone"),
    )
)
