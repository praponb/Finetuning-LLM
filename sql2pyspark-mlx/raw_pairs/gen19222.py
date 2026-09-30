from pyspark.sql import functions as F

parsed = (
    df_app_logs
    .filter(F.col("level").isin(["WARN", "ERROR"]))
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        F.col("context")["trace"].alias("trace"),
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["retries"].alias("retry_count"),
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
        F.coalesce(F.col("context").getItem("trace"), F.lit("none")).alias("trace_or_default"),
    )
)

result = (
    parsed
    .groupBy(F.date_trunc("DAY", "log_ts").alias("activity_day"), "service")
    .agg(
        F.count("*").alias("event_count"),
        F.sum(F.when(F.col("service") == "checkout", F.col("latency_ms")).otherwise(0)).alias("checkout_latency_ms"),
        F.sort_array(F.collect_set("service")).alias("service_list"),
        F.sum(F.when(F.col("service") == "billing", F.col("retry_count"))).alias("billing_total_retry_count"),
        F.count(F.when(F.col("level") == "INFO", 1)).alias("info_rows"),
    )
    .filter(F.col("info_rows") < 1000)
    .orderBy(F.col("service").desc())
)
