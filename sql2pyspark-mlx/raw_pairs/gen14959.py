from pyspark.sql import functions as F

unpacked = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "latency_ms",
        "context",
        F.coalesce(F.col("context").getItem("trace"), F.lit("none")).alias("trace_or_default"),
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
    )
)

result = (
    unpacked
    .withColumn("hour_window", F.date_trunc("hour", "log_ts"))
    .withColumn("latency_ms_tier", F.when(F.col("latency_ms") >= 1860, "PREMIUM").otherwise("STANDARD"))
    .cube("hour_window", "latency_ms_tier")
    .agg(
        F.countDistinct("trace_or_default").alias("distinct_trace_or_defaults"),
        F.percentile_approx("latency_ms", 0.95).alias("latency_ms_p95"),
        F.count("*").alias("event_count"),
        F.size(F.collect_set("host")).alias("num_hosts"),
        F.grouping_id().alias("gid"),
    )
    .filter(F.col("latency_ms_p95") > 0)
    .orderBy(F.col("event_count").desc_nulls_first())
)
