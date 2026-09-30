from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "level",
        "status_code",
        F.explode_outer("context").alias("attr_key", "v"),
    )
)

result = (
    flattened
    .select(
        "id",
        "log_ts",
        "attr_key",
        "v",
        F.when(F.col("attr_key").rlike("^(trace|version)$"), "OTHER_ZONE").when(F.col("attr_key").rlike("^(region)$"), "CORE").otherwise("GROUP_B").alias("attr_key_zone"),
    )
    .orderBy(F.col("attr_key").desc())
)
