from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "status_code",
        F.explode_outer("context").alias("key", "attr_value"),
    )
)

unpacked = (
    lines
    .select(
        "id",
        "log_ts",
        "key",
        "attr_value",
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("flag_bit8"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

cleaned = (
    unpacked
    .filter((F.col("log_ts") >= F.to_timestamp(F.lit("2025-06-01 14:00:00"))) & F.col("key").isNotNull())
    .select(
        "*",
        (F.col("flag_bit8") * F.col("status_code_high")).alias("flag_bit8_times_status_code_high"),
        F.coalesce("key", F.lit("OTHER")).alias("resolved_key"),
    )
)

valid = (
    cleaned
    .filter(F.col("key").isNotNull())
    .select(
        "*",
        F.round("flag_bit8_times_status_code_high", 2).alias("flag_bit8_times_status_code_high_rounded"),
        F.greatest("flag_bit8", "status_code_high").alias("greatest_flag_bit8_status_code_high"),
    )
)

result = (
    valid
    .select(
        "id",
        "log_ts",
        "attr_value",
        "status_code_high",
        "flag_bit8_times_status_code_high",
        "resolved_key",
        "flag_bit8_times_status_code_high_rounded",
    )
)
