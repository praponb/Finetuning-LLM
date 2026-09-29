from pyspark.sql import functions as F

parsed = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        "payload_json",
        "context",
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("is_flagged"),
    )
)

result = (
    parsed
    .groupBy(
        F.when(F.col("is_flagged") >= 1, "HIGH").otherwise("LOW").alias("is_flagged_tier"),
        F.date_trunc("HOUR", "log_ts").alias("hour_window"),
        "host",
    )
    .agg(
        F.min("log_ts").alias("first_seen"),
        F.stddev_samp("latency_ms").alias("latency_ms_stddev"),
        F.size(F.collect_set("host")).alias("num_hosts"),
        F.round(F.sum("is_flagged") / F.nullif(F.count(F.when(F.col("service") == "checkout", 1)), F.lit(0)), 2).alias("is_flagged_per_checkout"),
        F.bit_or("latency_ms").alias("latency_ms_bit_or"),
    )
    .orderBy(F.col("is_flagged_per_checkout").desc_nulls_last())
)
