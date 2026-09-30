from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("key", "value"),
    )
)

flattened_attrs = (
    exploded
    .filter(F.to_date("log_ts") >= "2023-09-01")
    .select(
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        "value",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("flag_bit2"),
    )
)

valid = (
    flattened_attrs
    .filter(F.col("log_ts") > F.to_date(F.lit("2025-06-01")))
    .select(
        "*",
        (F.col("flag_bit2") / F.nullif("status_code_high", F.lit(0))).alias("flag_bit2_per_status_code_high"),
        F.to_date("log_ts").alias("log_ts_date"),
    )
)

matched_keys = (
    valid
    .select("host")
    .intersectAll(
        df_app_logs
        .filter(F.col("host") == "h1")
        .select("host")
    )
)

verified = (
    valid
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.host") == F.col("k.host")), "inner")
    .select("s.*")
)

grouped = (
    verified
    .groupingSets([["host", "level"], ["host"], []], "host", "level")
    .agg(
        F.sum(F.when(F.col("level") == "ERROR", F.col("flag_bit2_per_status_code_high")).otherwise(0)).alias("error_flag_bit2_per_status_code_high"),
        F.count(F.when(F.col("host") == "h3", 1)).alias("h3_rows"),
        F.round(F.sum("status_code_high") / F.nullif(F.count(F.when(F.col("host") == "h3", 1)), F.lit(0)), 2).alias("status_code_high_per_h3"),
    )
)

result = (
    grouped
    .select("host", "level", "h3_rows")
    .orderBy(F.col("host").desc())
)
