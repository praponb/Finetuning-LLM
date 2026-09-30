from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        F.explode("context").alias("key", "attr_value"),
    )
)

kept_keys = (
    flattened
    .select("host", "level")
    .exceptAll(
        df_app_logs
        .filter(F.col("status_code") != 431)
        .select("host", "level")
    )
)

verified = (
    flattened
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.host") == F.col("k.host")) & (F.col("s.level") == F.col("k.level")), "inner")
    .select("s.*")
)

base = (
    verified
    .filter(F.col("key").isNotNull())
    .select(
        "*",
        F.when(F.col("host") == "h4", 1).otherwise(0).alias("is_h4"),
        F.coalesce("key", F.lit("NONE")).alias("key_clean"),
    )
)

valid = (
    base
    .filter(F.col("is_h4") > 75)
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "key",
        "attr_value",
        "is_h4",
        F.when(F.col("service") == "billing", 1).otherwise(0).alias("is_billing"),
        F.coalesce("key", F.lit("N/A")).alias("key_filled"),
    )
)

result = (
    valid
    .groupBy("level")
    .agg(
        F.percentile_approx("latency_ms", 0.95).alias("latency_ms_p95"),
        F.count(F.when(F.col("key") == "trace", 1)).alias("trace_rows"),
        ((F.unix_timestamp(F.max("log_ts")) - F.unix_timestamp(F.min("log_ts"))) / 60).alias("span_minutes"),
        F.min("latency_ms").alias("min_latency_ms"),
        F.stddev_samp("latency_ms").alias("latency_ms_stddev"),
        F.min("log_ts").alias("first_log_ts"),
    )
    .orderBy(F.col("level").desc_nulls_first())
)
