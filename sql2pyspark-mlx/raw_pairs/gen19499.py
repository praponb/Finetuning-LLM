from pyspark.sql import functions as F

unpacked = (
    df_app_logs
    .filter(F.col("latency_ms") == 733)
    .select(
        "id",
        "latency_ms",
        "context",
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
        F.size("context").alias("num_context"),
        F.coalesce(F.col("context")["trace"], F.lit("unknown")).alias("trace_or_default"),
    )
)

result = (
    unpacked
    .groupBy(
        F.when(F.col("num_context") >= 63, "PREMIUM").otherwise("STANDARD").alias("num_context_tier"),
    )
    .agg(
        F.stddev_samp("latency_ms").alias("latency_ms_stddev"),
        F.avg("num_context").alias("avg_num_context"),
        F.countDistinct("trace_or_default").alias("distinct_trace_or_defaults"),
        F.count("*").alias("row_count"),
        F.round(F.avg("latency_ms"), 2).alias("avg_latency_ms"),
    )
    .orderBy("distinct_trace_or_defaults")
)
