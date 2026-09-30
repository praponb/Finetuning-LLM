from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode("context").alias("attr_key", "v"),
    )
)

unpacked = (
    exploded
    .filter(F.col("status_code") >= 404)
    .select(
        "id",
        "host",
        "log_ts",
        "status_code",
        "attr_key",
        (F.col("status_code").bitwiseAND(4) != 0).cast("int").alias("flag_bit4"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

remaining_keys = (
    unpacked
    .select("id")
    .intersect(
        df_app_logs
        .filter(F.col("status_code") != 426)
        .select("id")
    )
)

verified = (
    unpacked
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

valid = (
    verified
    .filter((F.col("log_ts") >= F.lit("2025-06-02").cast("date")) & (F.col("attr_key") == "region"))
    .select(
        "id",
        "host",
        "log_ts",
        "status_code",
        "attr_key",
        "flag_bit4",
        "status_code_high",
        F.initcap(F.trim("host")).alias("host_title"),
        F.length("host").alias("host_length"),
        F.coalesce("attr_key", F.lit("DIRECT")).alias("resolved_attr_key"),
    )
)

result = (
    valid
    .select(
        "id",
        "log_ts",
        "status_code",
        "attr_key",
        "status_code_high",
        "host_title",
        "host_length",
        F.when(F.col("attr_key").rlike("^(region|trace)$"), "OTHER_ZONE").when(F.col("attr_key").rlike("^(version)$"), "CORE").otherwise("EXTENDED").alias("attr_key_zone"),
    )
    .orderBy("attr_key")
)
