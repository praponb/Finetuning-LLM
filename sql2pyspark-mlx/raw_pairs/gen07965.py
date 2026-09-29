from pyspark.sql import functions as F

parsed = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        "payload_json",
        F.size("context").alias("num_context"),
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
    )
)

result = (
    parsed
    .cube(
        F.when(F.col("status_code") >= 410, "PREMIUM").otherwise("STANDARD").alias("status_code_tier"),
    )
    .agg(
        F.avg("num_context").alias("avg_num_context"),
        F.corr("status_code", "latency_ms").alias("corr_status_code_latency_ms"),
        F.count_if(F.col("host") == "h3").alias("h3_events"),
        ((F.unix_timestamp(F.max("log_ts")) - F.unix_timestamp(F.min("log_ts"))) / 60).alias("duration_min"),
        F.percentile_approx("status_code", 0.9).alias("status_code_p90"),
    )
    .orderBy(F.col("avg_num_context").desc())
)
