from pyspark.sql import functions as F

unpacked = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
        F.array_contains(F.map_keys("context"), "region").alias("has_region"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["retries"].alias("retry_count"),
        F.coalesce(F.col("context")["version"], F.lit("direct")).alias("version_or_default"),
    )
)

result = (
    unpacked
    .groupBy(
        F.date_trunc("HOUR", "log_ts").alias("hour_window"),
        F.when(F.col("latency_ms") >= 1891, "PREMIUM").otherwise("STANDARD").alias("latency_ms_tier"),
    )
    .agg(
        F.max("log_ts").alias("last_log_ts"),
        F.countDistinct("version_or_default").alias("distinct_version_or_defaults"),
        F.sum(F.when(F.col("host") == "h1", F.col("latency_ms")).otherwise(0)).alias("h1_latency_ms"),
        F.sort_array(F.collect_set("level")).alias("level_list"),
        F.count("*").alias("num_rows"),
        F.min("log_ts").alias("first_log_ts"),
    )
    .filter(F.col("distinct_version_or_defaults") >= 1)
)
