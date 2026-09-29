from pyspark.sql import functions as F

parsed = (
    df_app_logs
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        "status_code",
        "payload_json",
        "context",
        F.size("context").alias("num_context"),
        F.col("context")["region"].alias("region"),
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

result = (
    parsed
    .groupBy(
        F.when(F.col("status_code_high") >= 3, "LARGE").otherwise("SMALL").alias("status_code_high_tier"),
        "service",
    )
    .agg(
        F.avg("latency_ms").alias("avg_latency_ms"),
        F.avg(F.when(F.col("service") == "auth", F.col("num_context"))).alias("auth_avg_num_context"),
        F.count("*").alias("num_rows"),
        F.sort_array(F.collect_set("service")).alias("service_list"),
        F.corr("status_code", "latency_ms").alias("corr_status_code_latency_ms"),
        F.percentile_approx("latency_ms", [0.25, 0.5, 0.75]).alias("latency_ms_quartiles"),
    )
    .orderBy(F.col("avg_latency_ms").desc_nulls_last())
)
