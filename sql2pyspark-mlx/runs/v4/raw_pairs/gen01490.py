from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "value"),
    )
)

result = (
    flattened
    .select(
        "id",
        "log_ts",
        "level",
        F.when(F.col("level").rlike("^(ERROR|INFO)$"), "PRIORITY").otherwise("EXTENDED").alias("level_zone"),
    )
    .orderBy(F.col("id").asc_nulls_first())
)
