from pyspark.sql import functions as F

parsed = (
    df_app_logs
    .filter(F.col("log_ts") < "2023-08-01")
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.col("context")["version"].alias("version"),
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["user"]["tier"].alias("tier"),
    )
)

result = (
    parsed
    .cube("tier")
    .agg(
        F.sort_array(F.collect_set("level")).alias("level_list"),
        F.stddev_samp("latency_ms").alias("latency_ms_stddev"),
        F.stddev_samp("status_code").alias("status_code_stddev"),
        F.count("*").alias("event_count"),
        F.round(F.avg("status_code"), 2).alias("avg_status_code"),
        F.sum(F.when(F.col("tier") == "free", F.col("status_code")).otherwise(0)).alias("free_status_code"),
        F.grouping("tier").alias("grouping_id"),
    )
)
