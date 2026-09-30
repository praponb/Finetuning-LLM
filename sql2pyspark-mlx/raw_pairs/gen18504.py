from pyspark.sql import functions as F

unpacked = (
    df_app_logs
    .select(
        "id",
        "host",
        "level",
        "latency_ms",
        "payload_json",
        "context",
        F.coalesce(F.col("context")["region"], F.lit("direct")).alias("region_or_default"),
        F.col("context")["trace"].alias("trace"),
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("flag_bit2"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["retries"].alias("retry_count"),
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
    )
)

result = (
    unpacked
    .groupBy("level")
    .agg(
        F.max(F.when(F.col("host") == "h2", F.col("flag_bit2"))).alias("h2_max_flag_bit2"),
        F.count(F.when(F.col("level") == "DEBUG", 1)).alias("debug_count"),
        F.percentile_approx("flag_bit2", 0.9).alias("flag_bit2_p90"),
        F.round(F.avg("retry_count"), 2).alias("avg_retry_count"),
    )
    .filter(F.col("h2_max_flag_bit2") < 1000000)
)
