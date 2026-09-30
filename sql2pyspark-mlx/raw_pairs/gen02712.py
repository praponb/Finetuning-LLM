from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "v"),
    )
)

result = (
    exploded
    .select(
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        "attr_key",
        "v",
        F.when(F.col("attr_key").rlike("^(trace|region)$"), "EXTENDED").when(F.col("attr_key").rlike("^(version)$"), "GROUP_B").otherwise("GROUP_A").alias("attr_key_zone"),
    )
    .orderBy(F.col("attr_key").desc_nulls_last())
)
