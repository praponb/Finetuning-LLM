from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "latency_ms",
        F.explode_outer("context").alias("k", "attr_value"),
    )
)

cleaned = (
    lines
    .filter((F.col("log_ts") > F.lit("2025-06-01").cast("date")) & F.col("k").isNotNull())
    .select(
        "id",
        "service",
        "log_ts",
        "latency_ms",
        "k",
        "attr_value",
        F.col("log_ts").cast("date").alias("log_ts_day"),
        F.coalesce("k", F.lit("OTHER")).alias("k_clean"),
    )
)

filtered = (
    cleaned
    .filter(F.col("k").eqNullSafe(F.lit("region")))
    .select(
        "id",
        "service",
        "latency_ms",
        "k",
        "attr_value",
        "log_ts_day",
        F.last_day("log_ts_day").alias("log_ts_day_month_end"),
        F.when(F.col("latency_ms") >= 2478, "High").when(F.col("latency_ms") >= 1154, "Medium").otherwise("Low").alias("latency_ms_tier"),
    )
)

filtered_app_logs = (
    filtered
    .filter(F.year("log_ts_day") == 2024)
    .select(
        "*",
        F.date_format("log_ts_day", "yyyy-MM").alias("log_ts_day_label"),
        F.coalesce("k", F.lit("unassigned")).alias("k_filled"),
        F.coalesce("k", F.lit("UNKNOWN")).alias("k_filled_2"),
    )
)

result = (
    filtered_app_logs
    .select(
        "service",
        "k",
        "attr_value",
        "log_ts_day",
        "log_ts_day_month_end",
        "latency_ms_tier",
        "log_ts_day_label",
        "k_filled",
        "k_filled_2",
    )
)
