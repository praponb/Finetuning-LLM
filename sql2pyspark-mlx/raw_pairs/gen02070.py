from pyspark.sql import functions as F

parsed = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        "payload_json",
        F.coalesce(F.col("context")["version"], F.lit("unknown")).alias("version_or_default"),
        F.coalesce(F.col("context")["trace"], F.lit("none")).alias("trace_or_default"),
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
    )
)

result = (
    parsed
    .groupBy(F.date_trunc("HOUR", "log_ts").alias("hour_window"), "service")
    .agg(
        F.max("log_ts").alias("last_log_ts"),
        F.stddev_samp("latency_ms").alias("latency_ms_stddev"),
        F.bit_and("latency_ms").alias("latency_ms_bit_and"),
        F.min("log_ts").alias("first_seen"),
        F.size(F.collect_set("service")).alias("num_services"),
    )
    .filter(F.col("num_services") > 1)
)
