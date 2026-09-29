from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        F.explode("context").alias("key", "value"),
    )
)

cleaned = (
    exploded
    .filter(F.col("key").isNotNull() & (~F.col("key").eqNullSafe(F.lit("version"))))
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        "key",
        "value",
        F.initcap(F.trim("service")).alias("service_title"),
        F.coalesce("key", F.lit("NONE")).alias("resolved_key"),
    )
)

filtered = (
    cleaned
    .filter(F.col("key").isNotNull() & (~F.col("key").eqNullSafe(F.lit("region"))))
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        "key",
        "value",
        "service_title",
        "resolved_key",
        F.when(F.col("latency_ms") >= 2035, "High").when(F.col("latency_ms") >= 1125, "Medium").otherwise("Low").alias("latency_ms_tier"),
    )
)

filtered_app_logs = (
    filtered
    .filter((~F.col("key").eqNullSafe(F.lit("trace"))) & (F.col("level") == "ERROR"))
    .withColumn("level_length", F.length("level"))
    .withColumn("service_title_2", F.initcap(F.trim("service")))
)

result = (
    filtered_app_logs
    .groupingSets([["key", "service"], ["key"]], "key", "service")
    .agg(
        F.count(F.when(F.col("service") == "checkout", 1)).alias("checkout_count"),
        F.count(F.when(F.col("level") == "DEBUG", 1)).alias("debug_rows"),
        F.max("level_length").alias("max_level_length"),
        F.max("latency_ms").alias("max_latency_ms"),
        F.round(F.sum("level_length") / F.nullif(F.count(F.when(F.col("service") == "search", 1)), F.lit(0)), 2).alias("level_length_per_search"),
    )
)
