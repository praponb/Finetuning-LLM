from pyspark.sql import functions as F

unpacked = (
    df_app_logs
    .select(
        "id",
        "level",
        "context",
        F.col("context")["trace"].alias("trace"),
        F.coalesce(F.col("context")["trace"], F.lit("unknown")).alias("trace_or_default"),
        F.col("context").getItem("version").alias("version"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["retries"].alias("retry_count"),
    )
)

result = (
    unpacked
    .groupBy(
        F.when(F.col("retry_count") >= 4, "HIGH").otherwise("LOW").alias("retry_count_tier"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.percentile_approx("duration_sec", 0.9).alias("duration_sec_p90"),
        F.count(F.when(F.col("level") == "DEBUG", 1)).alias("debug_count"),
        F.count(F.when(F.col("level") == "ERROR", 1)).alias("error_rows"),
        F.sum(F.when(F.col("level") == "DEBUG", F.col("retry_count")).otherwise(0)).alias("debug_retry_count"),
        F.size(F.collect_set("level")).alias("num_levels"),
    )
    .orderBy(F.col("num_levels").asc_nulls_last())
)
