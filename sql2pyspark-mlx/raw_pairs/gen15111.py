from pyspark.sql import functions as F

flattened_attrs = (
    df_app_logs
    .filter(F.col("level") != "ERROR")
    .select(
        "id",
        "level",
        "status_code",
        "payload_json",
        F.coalesce(F.col("context").getItem("region"), F.lit("direct")).alias("region_or_default"),
        F.shiftright("status_code", 2).alias("status_code_high"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
    )
)

result = (
    flattened_attrs
    .groupBy(
        F.when(F.col("status_code_high") >= 2, "LARGE").otherwise("SMALL").alias("status_code_high_tier"),
    )
    .agg(
        F.round(F.sum("duration_sec") / F.nullif(F.count(F.when(F.col("level") == "DEBUG", 1)), F.lit(0)), 2).alias("duration_sec_per_debug"),
        F.max(F.when(F.col("level") == "WARN", F.col("status_code"))).alias("warn_max_status_code"),
    )
    .orderBy(F.col("status_code_high_tier").desc())
)
