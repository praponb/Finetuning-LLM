from pyspark.sql import functions as F

parsed = (
    df_app_logs
    .filter(F.col("status_code").between(276, 423))
    .select(
        "id",
        "host",
        "latency_ms",
        "status_code",
        "payload_json",
        "context",
        F.col("context").getItem("region").alias("region"),
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
        F.col("context")["trace"].alias("trace"),
    )
)

result = (
    parsed
    .groupBy(
        F.when(F.col("latency_ms") >= 2445, "PREMIUM").otherwise("STANDARD").alias("latency_ms_tier"),
    )
    .agg(
        F.count(F.when(F.col("host") == "h2", 1)).alias("h2_rows"),
        F.corr("latency_ms", "status_code").alias("corr_latency_ms_status_code"),
        F.stddev_samp("status_code").alias("status_code_stddev"),
        F.corr("status_code", "latency_ms").alias("corr_status_code_latency_ms"),
    )
)
