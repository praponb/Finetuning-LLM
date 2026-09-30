from pyspark.sql import functions as F

unpacked = (
    df_app_logs
    .select(
        "id",
        "service",
        "status_code",
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["retries"].alias("retry_count"),
        F.coalesce(F.col("context")["trace"], F.lit("unknown")).alias("trace_or_default"),
        F.size("context").alias("num_context"),
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
    )
)

result = (
    unpacked
    .groupBy("service")
    .agg(
        F.sort_array(F.collect_set("service")).alias("service_list"),
        F.bit_or("status_code").alias("status_code_bit_or"),
        F.count("*").alias("num_rows"),
        F.count(F.when(F.col("service") == "auth", 1)).alias("auth_count"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("num_rows") >= 1)
    .orderBy(F.col("status_code_bit_or").desc_nulls_last())
)
