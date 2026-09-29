from pyspark.sql import functions as F

flattened_attrs = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "payload_json",
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
        F.coalesce(F.col("context")["version"], F.lit("unknown")).alias("version_or_default"),
    )
)

result = (
    flattened_attrs
    .groupBy(
        F.when(F.col("duration_sec") >= 3.6, "LARGE").otherwise("SMALL").alias("duration_sec_tier"),
        "service",
    )
    .agg(
        F.sum(F.when(F.col("service") == "auth", F.col("duration_sec")).otherwise(0)).alias("auth_duration_sec"),
        F.count("*").alias("num_rows"),
        ((F.unix_timestamp(F.max("log_ts")) - F.unix_timestamp(F.min("log_ts"))) / 60).alias("span_minutes"),
        F.count_if(F.col("service") == "checkout").alias("checkout_events"),
    )
    .filter(F.col("checkout_events") > 1)
    .orderBy(F.col("duration_sec_tier").desc())
)
