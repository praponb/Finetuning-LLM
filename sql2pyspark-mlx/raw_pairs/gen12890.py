from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select("id", "log_ts", "level", F.explode_outer("context").alias("attr_key", "v"))
)

result = (
    lines
    .select(
        "id",
        "log_ts",
        "level",
        "attr_key",
        "v",
        F.when(F.col("attr_key").rlike("^(region|version)$"), "GROUP_A").otherwise("PRIORITY").alias("attr_key_zone"),
    )
    .orderBy("level")
)
