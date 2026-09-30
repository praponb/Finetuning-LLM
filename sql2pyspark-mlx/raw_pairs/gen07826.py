from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "attr_value"),
    )
)

result = (
    flattened
    .select(
        "id",
        "level",
        "latency_ms",
        "attr_key",
        "attr_value",
        F.when(F.col("level").rlike("^(WARN|INFO)$"), "OTHER_ZONE").when(F.col("level").rlike("^(ERROR|DEBUG)$"), "PRIORITY").otherwise("EXTENDED").alias("level_zone"),
    )
    .orderBy(F.col("id").desc())
)
