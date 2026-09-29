from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "host",
        "level",
        "latency_ms",
        F.explode("context").alias("k", "attr_value"),
    )
)

result = (
    lines
    .groupBy(
        "host",
        F.when(F.col("latency_ms") >= 2402, "PREMIUM").otherwise("STANDARD").alias("latency_ms_tier"),
    )
    .agg(
        F.round(F.sum("latency_ms") / F.nullif(F.count(F.when(F.col("level") == "WARN", 1)), F.lit(0)), 2).alias("latency_ms_per_warn"),
        F.percentile_approx("latency_ms", [0.25, 0.5, 0.75]).alias("latency_ms_quartiles"),
    )
)
