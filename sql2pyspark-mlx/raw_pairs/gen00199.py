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

flattened_attrs = (
    flattened
    .filter(F.col("status_code") < 410)
    .select(
        "service",
        "latency_ms",
        "status_code",
        "value",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("flag_bit2"),
    )
)

base = (
    flattened_attrs
    .filter(F.col("status_code_high") != 2)
    .select(
        "*",
        F.round((F.col("flag_bit2") * 100), 2).alias("flag_bit2_adjusted"),
        F.round((F.col("status_code_high") / F.col("status_code")), 2).alias("status_code_high_per_status_code"),
    )
)

cleaned = (
    base
    .filter(F.col("status_code_high_per_status_code") > 53)
    .select(
        "latency_ms",
        "status_code",
        "value",
        "status_code_high",
        "flag_bit2",
        F.when(F.col("flag_bit2") >= 1, "High").when(F.col("flag_bit2") >= 0, "Medium").otherwise("Low").alias("flag_bit2_band"),
        F.least("status_code_high", "flag_bit2").alias("least_status_code_high_flag_bit2"),
    )
)

grouped = (
    cleaned
    .groupBy(
        F.when(F.col("latency_ms") >= 1977, "HIGH").otherwise("LOW").alias("latency_ms_tier"),
        F.when(F.col("status_code_high") >= 2, "HIGH").otherwise("LOW").alias("status_code_high_tier"),
    )
    .agg(
        F.percentile_approx("status_code_high", [0.25, 0.5, 0.75]).alias("status_code_high_quartiles"),
        F.round(F.avg("status_code_high"), 2).alias("avg_status_code_high"),
        F.percentile_approx("status_code_high", 0.5).alias("status_code_high_p50"),
        F.avg("status_code_high").alias("avg_status_code_high_2"),
        F.stddev_samp("status_code").alias("status_code_stddev"),
    )
    .filter(F.col("status_code_stddev") > 0)
)

result = (
    grouped
    .select(
        "latency_ms_tier",
        "status_code_high_tier",
        "avg_status_code_high_2",
        "status_code_stddev",
    )
)
