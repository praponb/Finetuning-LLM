from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        F.explode_outer("context").alias("key", "value"),
    )
)

result = (
    lines
    .select(
        "id",
        "log_ts",
        "level",
        "key",
        F.when(F.col("level").rlike("^(WARN|ERROR)$"), "GROUP_A").when(F.col("level").rlike("^(INFO|DEBUG)$"), "EXTENDED").otherwise("PRIORITY").alias("level_zone"),
    )
    .orderBy(F.col("level_zone").desc())
)
