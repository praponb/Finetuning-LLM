from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("key", "v"),
    )
)

result = (
    flattened
    .withColumn("hour_window", F.date_trunc("HOUR", "log_ts"))
    .withColumn("status_code_tier", F.when(F.col("status_code") >= 432, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["hour_window", "status_code_tier"], ["hour_window"]],
        "hour_window",
        "status_code_tier",
    )
    .agg(
        F.count(F.when(F.col("key") == "version", 1)).alias("version_rows"),
        F.count(F.when(F.col("level") == "DEBUG", 1)).alias("debug_count"),
    )
    .filter(F.col("version_rows") > 1)
)
