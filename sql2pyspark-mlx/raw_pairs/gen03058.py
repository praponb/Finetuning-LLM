from pyspark.sql import functions as F

flattened_attrs = (
    df_app_logs
    .filter(F.col("level") == "INFO")
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        "context",
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
        F.col("context")["version"].alias("version"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["user"]["tier"].alias("tier"),
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("flag_bit2"),
    )
)

result = (
    flattened_attrs
    .groupBy(F.date_trunc("DAY", "log_ts").alias("activity_day"), "service")
    .agg(
        F.size(F.collect_set("service")).alias("num_services"),
        F.bit_and("latency_ms").alias("latency_ms_bit_and"),
        F.bit_or("latency_ms").alias("latency_ms_bit_or"),
        F.min("log_ts").alias("first_seen"),
    )
)
