from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "host",
        "level",
        "latency_ms",
        F.explode_outer("context").alias("key", "attr_value"),
    )
)

result = (
    exploded
    .select(
        "id",
        "host",
        "level",
        "key",
        "attr_value",
        F.when(F.col("level").rlike("^(WARN|INFO)$"), "PRIORITY").when(F.col("level").rlike("^(ERROR|DEBUG)$"), "CORE").otherwise("GROUP_B").alias("level_zone"),
    )
    .orderBy(F.col("level").desc())
)
