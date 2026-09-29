from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        F.explode("context").alias("attr_key", "v"),
    )
)

result = (
    lines
    .select(
        "id",
        "log_ts",
        "latency_ms",
        "attr_key",
        "v",
        F.when(F.col("attr_key").rlike("^(version|trace)$"), "GROUP_A").when(F.col("attr_key").rlike("^(region)$"), "CORE").otherwise("EXTENDED").alias("attr_key_zone"),
    )
)
