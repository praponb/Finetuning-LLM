from pyspark.sql import functions as F

parsed = (
    df_app_logs
    .filter(F.col("service") == "billing")
    .select(
        "id",
        "service",
        "host",
        "status_code",
        "payload_json",
        F.array_contains(F.map_keys("context"), "version").alias("has_version"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["retries"].alias("retry_count"),
        F.col("context").getItem("trace").alias("trace"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

result = (
    parsed
    .groupBy(
        F.when(F.col("status_code") >= 426, "HIGH").otherwise("LOW").alias("status_code_tier"),
    )
    .agg(
        F.percentile_approx("status_code", 0.95).alias("status_code_p95"),
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("trace").alias("distinct_traces"),
        F.count(F.when(F.col("service") == "search", 1)).alias("search_rows"),
        F.sort_array(F.collect_set("host")).alias("host_list"),
    )
    .orderBy(F.col("distinct_ids").asc_nulls_first())
)
