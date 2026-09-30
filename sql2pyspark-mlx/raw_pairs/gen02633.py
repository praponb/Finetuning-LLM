from pyspark.sql import functions as F

parsed = (
    df_app_logs
    .select(
        "id",
        "level",
        "status_code",
        "payload_json",
        F.array_contains(F.map_keys("context"), "region").alias("has_region"),
        F.coalesce(F.col("context").getItem("trace"), F.lit("direct")).alias("trace_or_default"),
        F.shiftright("status_code", 2).alias("status_code_high"),
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
    )
)

result = (
    parsed
    .groupBy("level")
    .agg(
        F.countDistinct("trace_or_default").alias("distinct_trace_or_defaults"),
        F.max(F.when(F.col("level") == "WARN", F.col("status_code_high"))).alias("warn_max_status_code_high"),
        F.count("*").alias("event_count"),
        F.percentile_approx("status_code", 0.95).alias("status_code_p95"),
        F.bit_and("status_code").alias("status_code_bit_and"),
    )
    .orderBy(F.col("status_code_p95").desc())
)
