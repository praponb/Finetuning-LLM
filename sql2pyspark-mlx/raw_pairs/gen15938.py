from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        F.explode("context").alias("k", "attr_value"),
    )
)

filtered = (
    lines
    .filter(F.col("k").isin(["version", "region"]))
    .select(
        "id",
        "service",
        "level",
        "k",
        "attr_value",
        F.upper("level").alias("level_upper"),
        F.upper("k").alias("k_upper"),
        F.coalesce("k", F.lit("NONE")).alias("k_clean"),
    )
)

valid = (
    filtered
    .filter(F.col("k").isNotNull())
    .select(
        "id",
        "level",
        "k",
        "level_upper",
        F.when(F.col("k") == "trace", 1).otherwise(0).alias("is_trace"),
        F.concat_ws("/", "service", "k").alias("service_k_key"),
        F.coalesce("k", F.lit("NONE")).alias("k_filled"),
    )
)

base = (
    valid
    .filter(F.col("k").isNotNull() & (F.col("k") == "version"))
    .select(
        "id",
        "k",
        "level_upper",
        "is_trace",
        "service_k_key",
        F.coalesce("is_trace", F.lit(0)).alias("is_trace_filled"),
        F.substring("level", 1, 2).alias("level_prefix"),
    )
)

filtered_app_logs = (
    base
    .filter(F.col("is_trace").between(28, 52))
    .select(
        "id",
        "k",
        "level_upper",
        "is_trace",
        "is_trace_filled",
        "level_prefix",
        F.lower("k").alias("k_lower"),
        F.coalesce("is_trace_filled", F.lit(0)).alias("is_trace_filled_filled"),
    )
)

result = (
    filtered_app_logs
    .select(
        "id",
        "k",
        "level_upper",
        "is_trace_filled",
        "k_lower",
        "is_trace_filled_filled",
    )
    .orderBy("k")
)
