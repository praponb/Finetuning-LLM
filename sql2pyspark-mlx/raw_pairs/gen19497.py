from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        F.explode_outer("context").alias("key", "attr_value"),
    )
)

valid = (
    flattened
    .filter(F.col("key").isNotNull() & (F.col("log_ts") > F.lit("2025-06-01 06:00:00").cast("timestamp")))
    .select("*", F.concat("key", F.lit(", "), "level").alias("key_level"))
)

filtered = (
    valid
    .filter(~F.col("key").eqNullSafe(F.lit("region")))
    .withColumn("log_ts_period", F.date_trunc("YEAR", "log_ts"))
    .withColumn("key_clean", F.coalesce("key", F.lit("NONE")))
)

cleaned = (
    filtered
    .filter(F.col("key").isNotNull())
    .withColumn("is_debug", F.when(F.col("level") == "DEBUG", 1).otherwise(0))
    .withColumn("key_filled", F.coalesce("key", F.lit("NONE")))
)

filtered_app_logs = (
    cleaned
    .filter(F.col("key").isNotNull() & (F.col("log_ts") >= F.lit("2025-06-01 08:00:00").cast("timestamp")))
    .select(
        "*",
        F.trim("host").alias("clean_host"),
        F.coalesce("key", F.lit("UNKNOWN")).alias("key_clean_2"),
    )
)

totals = (
    filtered_app_logs
    .groupBy(
        F.when(F.col("is_debug") >= 79, "LARGE").otherwise("SMALL").alias("is_debug_tier"),
    )
    .agg(
        F.min("log_ts").alias("started_at"),
        F.count(F.when(F.col("key") == "version", 1)).alias("version_count"),
    )
)

result = totals.select("is_debug_tier", "started_at", "version_count")
