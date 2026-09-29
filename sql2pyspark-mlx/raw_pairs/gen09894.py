from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "latency_ms",
        F.explode_outer("context").alias("attr_key", "value"),
    )
)

result = (
    lines
    .select(
        "id",
        "log_ts",
        "attr_key",
        "value",
        F.when(F.col("attr_key").rlike("^(region|trace)$"), "GROUP_A").when(F.col("attr_key").rlike("^(version)$"), "CORE").otherwise("EXTENDED").alias("attr_key_zone"),
    )
    .orderBy(F.col("attr_key").desc(), "attr_key_zone")
)
