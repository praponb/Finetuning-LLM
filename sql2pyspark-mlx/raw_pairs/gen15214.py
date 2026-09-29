from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode("context").alias("attr_key", "value"),
    )
)

result = (
    lines
    .select(
        "id",
        "service",
        "attr_key",
        "value",
        F.when(F.col("attr_key").rlike("^(version|region)$"), "OTHER_ZONE").when(F.col("attr_key").rlike("^(trace)$"), "GROUP_A").otherwise("CORE").alias("attr_key_zone"),
    )
)
