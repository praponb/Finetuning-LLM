from pyspark.sql import functions as F

unpacked = (
    df_app_logs
    .select(
        "id",
        "latency_ms",
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
        F.coalesce(F.col("context").getItem("trace"), F.lit("direct")).alias("trace_or_default"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["user"]["tier"].alias("tier"),
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
    )
)

result = (
    unpacked
    .groupBy(
        F.when(F.col("latency_ms") >= 2502, "LARGE").otherwise("SMALL").alias("latency_ms_tier"),
        "tier",
    )
    .agg(
        F.sum(F.when(F.col("tier") == "gold", F.col("latency_ms")).otherwise(0)).alias("gold_latency_ms"),
        F.percentile_approx("latency_ms", 0.95).alias("latency_ms_p95"),
        F.avg(F.when(F.col("tier") == "pro", F.col("latency_ms"))).alias("pro_avg_latency_ms"),
        F.count_if(F.col("tier") == "free").alias("free_events"),
        F.size(F.collect_set("tier")).alias("num_tiers"),
        F.percentile_approx("latency_ms", 0.95).alias("latency_ms_p95_2"),
    )
    .orderBy("num_tiers")
)
