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
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        (F.col("status_code").bitwiseAND(4) != 0).cast("int").alias("flag_bit4"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

valid = (
    flattened_attrs
    .filter(F.col("level").isin(["INFO", "ERROR", "DEBUG"]))
    .select(
        "id",
        "level",
        "latency_ms",
        "flag_bit4",
        "status_code_high",
        (F.col("flag_bit4") / F.nullif("status_code_high", F.lit(0))).alias("flag_bit4_per_status_code_high"),
        F.concat_ws("/", "service", "level").alias("service_level_key"),
    )
)

cleaned = (
    valid
    .filter(F.col("latency_ms") < 2136)
    .select(
        "id",
        "level",
        "latency_ms",
        "flag_bit4",
        "status_code_high",
        "flag_bit4_per_status_code_high",
        "service_level_key",
        F.trim("level").alias("clean_level"),
    )
)

base = (
    cleaned
    .filter(F.col("status_code_high").between(1, 2))
    .withColumn("flag_bit4_rounded", F.round("flag_bit4", 1))
)

result = (
    base
    .select(
        "id",
        "level",
        "latency_ms",
        "flag_bit4",
        "status_code_high",
        "clean_level",
        "flag_bit4_rounded",
    )
    .orderBy("level")
)
