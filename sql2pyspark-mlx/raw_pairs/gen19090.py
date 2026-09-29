from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        F.explode_outer("context").alias("key", "attr_value"),
    )
)

base = (
    flattened
    .filter(F.col("log_ts") >= F.lit("2025-06-01 12:00:00").cast("timestamp"))
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "key",
        F.upper("host").alias("host_upper"),
        F.coalesce("key", F.lit("OTHER")).alias("resolved_key"),
    )
)

matched_keys = (
    base
    .select("id", "host")
    .intersectAll(
        df_app_logs
        .filter(F.col("level") == "DEBUG")
        .select("id", "host")
    )
)

verified = (
    base
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.host") == F.col("k.host")), "inner")
    .select("s.*")
)

valid = (
    verified
    .filter((F.col("log_ts") > F.lit("2025-06-01 12:00:00").cast("timestamp")) & F.col("key").isNotNull())
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "key",
        "resolved_key",
        F.initcap(F.trim("key")).alias("key_title"),
        F.lower("host").alias("host_lower"),
        F.coalesce("key", F.lit("DIRECT")).alias("key_filled"),
    )
)

result = (
    valid
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "resolved_key",
        "key_title",
        "host_lower",
        "key_filled",
    )
    .orderBy(F.col("host").desc_nulls_last())
)
