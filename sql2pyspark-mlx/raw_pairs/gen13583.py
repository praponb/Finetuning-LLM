from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("k", "attr_value"),
    )
)

parsed = (
    flattened
    .select(
        "id",
        "host",
        "k",
        "attr_value",
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("is_flagged"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

remaining_keys = (
    parsed
    .select("id")
    .subtract(
        df_app_logs
        .filter(F.col("status_code") <= 349)
        .select("id")
    )
)

reconciled = (
    parsed
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

valid = (
    reconciled
    .filter(F.col("k").isNotNull())
    .select(
        "*",
        F.col("is_flagged").cast("double").alias("is_flagged_num"),
        F.substring("host", 1, 4).alias("host_prefix"),
    )
)

result = (
    valid
    .select(
        "id",
        "host",
        "attr_value",
        "status_code_high",
        "is_flagged_num",
        "host_prefix",
        F.when(F.col("host").rlike("^(h1|h3)$"), "OTHER_ZONE").otherwise("GROUP_B").alias("host_zone"),
    )
    .orderBy("id")
)
