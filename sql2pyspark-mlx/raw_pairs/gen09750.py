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
        "status_code",
        F.explode_outer("context").alias("k", "value"),
    )
)

parsed = (
    flattened
    .select(
        "host",
        "level",
        "status_code",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("is_flagged"),
    )
)

filtered = (
    parsed
    .filter(F.col("host") != "h4")
    .select("*", F.substring("level", 1, 4).alias("level_prefix"))
)

base = (
    filtered
    .filter(F.col("status_code") > 282)
    .select(
        "host",
        "level",
        "status_code",
        "status_code_high",
        "is_flagged",
        "level_prefix",
        F.trim("host").alias("clean_host"),
        F.length("host").alias("host_length"),
    )
)

summary = (
    base
    .cube("host", "level")
    .agg(
        F.countDistinct("level").alias("distinct_levels"),
        F.countDistinct("level").alias("distinct_levels_2"),
    )
)

result = summary.select("host", "distinct_levels")
