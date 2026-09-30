from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("key", "v"),
    )
)

unpacked = (
    lines
    .filter(F.col("level") == "INFO")
    .select(
        "id",
        "level",
        "latency_ms",
        "key",
        "v",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(4) != 0).cast("int").alias("flag_bit4"),
    )
)

valid = (
    unpacked
    .filter(F.col("key").isNotNull() & (~F.col("key").eqNullSafe(F.lit("region"))))
    .withColumn("flag_bit4_per_latency_ms", F.round((F.col("flag_bit4") / F.col("latency_ms")), 2))
)

valid_app_logs = (
    valid
    .filter(~F.col("key").isin(["version", "region"]))
    .select(
        "*",
        (F.col("latency_ms") / F.nullif("status_code_high", F.lit(0))).alias("latency_ms_per_status_code_high"),
        F.round((F.col("latency_ms") * 1.1), 2).alias("latency_ms_adjusted"),
    )
)

valid_app_logs_2 = (
    valid_app_logs
    .filter(F.col("key").isNotNull())
    .select(
        "id",
        "level",
        "latency_ms",
        "key",
        "v",
        "status_code_high",
        "flag_bit4",
        "flag_bit4_per_latency_ms",
        "latency_ms_per_status_code_high",
        "latency_ms_adjusted",
        F.round((F.col("latency_ms") * 1.1), 2).alias("latency_ms_scaled"),
    )
)

result = (
    valid_app_logs_2
    .select(
        "id",
        "level",
        "latency_ms",
        "v",
        "status_code_high",
        "flag_bit4",
        "flag_bit4_per_latency_ms",
        "latency_ms_per_status_code_high",
        "latency_ms_scaled",
        F.when(F.col("level").rlike("^(INFO|DEBUG)$"), "PRIORITY").when(F.col("level").rlike("^(WARN|ERROR)$"), "GROUP_B").otherwise("GROUP_A").alias("level_zone"),
    )
    .orderBy(F.col("id").asc_nulls_first(), F.col("status_code_high").desc())
)
