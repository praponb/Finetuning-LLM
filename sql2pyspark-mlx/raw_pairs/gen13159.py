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
        F.explode_outer("context").alias("k", "v"),
    )
)

result = (
    lines
    .groupBy(
        F.when(F.col("status_code") >= 429, "PREMIUM").otherwise("STANDARD").alias("status_code_tier"),
    )
    .agg(
        F.round(F.sum("status_code") / F.nullif(F.count(F.when(F.col("level") == "DEBUG", 1)), F.lit(0)), 2).alias("status_code_per_debug"),
        ((F.unix_timestamp(F.max("log_ts")) - F.unix_timestamp(F.min("log_ts"))) / 60).alias("duration_min"),
        F.min("log_ts").alias("first_log_ts"),
        F.max("latency_ms").alias("max_latency_ms"),
        F.percentile_approx("latency_ms", [0.25, 0.5, 0.75]).alias("latency_ms_quartiles"),
    )
)
