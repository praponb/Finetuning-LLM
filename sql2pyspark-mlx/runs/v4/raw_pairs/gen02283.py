from pyspark.sql import functions as F

flattened_attrs = (
    df_app_logs
    .filter(F.col("service").isin(["billing", "search", "checkout"]))
    .select(
        "id",
        "service",
        "level",
        "payload_json",
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["retries"].alias("retry_count"),
        F.array_contains(F.map_keys("context"), "version").alias("has_version"),
    )
)

result = (
    flattened_attrs
    .groupBy(
        F.when(F.col("retry_count") >= 3, "PREMIUM").otherwise("STANDARD").alias("retry_count_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("retry_count", 0.95).alias("retry_count_p95"),
        F.avg(F.when(F.col("level") == "DEBUG", F.col("duration_sec"))).alias("debug_avg_duration_sec"),
        F.sum(F.when(F.col("level") == "DEBUG", F.col("retry_count"))).alias("debug_total_retry_count"),
    )
    .orderBy(F.col("debug_avg_duration_sec").desc_nulls_last())
)
