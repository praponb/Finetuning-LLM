from pyspark.sql import functions as F

parsed = (
    df_app_logs
    .filter(F.col("host") == "h1")
    .select(
        "id",
        "service",
        "host",
        "latency_ms",
        "status_code",
        "payload_json",
        "context",
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["retries"].alias("retry_count"),
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("is_flagged"),
    )
)

result = (
    parsed
    .groupBy("host")
    .agg(
        F.percentile_approx("is_flagged", [0.25, 0.5, 0.75]).alias("is_flagged_quartiles"),
        F.count(F.when(F.col("service") == "checkout", 1)).alias("checkout_count"),
        F.bit_and("status_code").alias("status_code_bit_and"),
    )
    .orderBy(F.col("host").desc_nulls_first())
)
