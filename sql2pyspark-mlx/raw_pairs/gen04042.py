from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        F.explode("context").alias("attr_key", "v"),
    )
)

result = (
    flattened
    .select(
        "id",
        "service",
        "level",
        "attr_key",
        "v",
        F.when(F.col("level").rlike("^(ERROR|DEBUG)$"), "GROUP_B").when(F.col("level").rlike("^(INFO|WARN)$"), "GROUP_A").otherwise("PRIORITY").alias("level_zone"),
    )
    .orderBy(F.col("service").desc())
)
