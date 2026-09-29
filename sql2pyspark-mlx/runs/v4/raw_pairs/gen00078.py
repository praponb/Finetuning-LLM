from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("key", "v"),
    )
)

flattened_attrs = (
    lines
    .select(
        "latency_ms",
        "status_code",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(4) != 0).cast("int").alias("has_fault"),
    )
)

base = (
    flattened_attrs
    .filter(F.col("status_code_high") < 2)
    .select("*", F.round("status_code", 2).alias("status_code_rounded"))
)

filtered = (
    base
    .filter(F.col("status_code_rounded") > 77)
    .select(
        "latency_ms",
        "status_code",
        "has_fault",
        "status_code_rounded",
        F.col("status_code").cast("string").alias("status_code_str"),
    )
)

valid = (
    filtered
    .filter(F.col("status_code").between(328, 433))
    .select(
        "status_code",
        "status_code_rounded",
        F.col("status_code").cast("string").alias("status_code_num"),
    )
)

filtered_app_logs = (
    valid
    .filter(F.col("status_code").between(264, 379))
    .select(
        "*",
        (F.col("status_code_rounded") - F.col("status_code")).alias("status_code_rounded_minus_status_code"),
    )
)

result = (
    filtered_app_logs
    .select(
        "status_code",
        "status_code_rounded",
        "status_code_num",
        "status_code_rounded_minus_status_code",
    )
    .orderBy("status_code_rounded_minus_status_code")
)
