from pyspark.sql import functions as F

unpacked = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.col("context").getItem("trace").alias("trace"),
        F.shiftright("status_code", 2).alias("status_code_high"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["user"]["tier"].alias("tier"),
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["retries"].alias("retry_count"),
    )
)

result = (
    unpacked
    .groupBy(F.date_trunc("HOUR", "log_ts").alias("hour_window"))
    .agg(
        F.round(F.avg("status_code_high"), 2).alias("avg_status_code_high"),
        F.percentile_approx("latency_ms", 0.5).alias("latency_ms_p50"),
        F.sort_array(F.collect_set("tier")).alias("tier_list"),
        F.count_if(F.col("tier") == "free").alias("free_events"),
        F.round(F.sum("latency_ms") / F.nullif(F.count(F.when(F.col("tier") == "pro", 1)), F.lit(0)), 2).alias("latency_ms_per_pro"),
    )
    .filter(F.col("latency_ms_per_pro") < 1000000)
)
