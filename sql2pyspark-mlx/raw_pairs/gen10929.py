from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "value"),
    )
)

valid = (
    exploded
    .filter(F.col("log_ts") >= F.lit("2025-06-01 13:00:00").cast("timestamp"))
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.length("k").alias("k_length"),
    )
)

base = (
    valid
    .filter(F.col("log_ts") >= F.lit("2025-06-01 09:00:00").cast("timestamp"))
    .withColumn("latency_ms_adjusted", F.round((F.col("latency_ms") * 0.85), 2))
)

cleaned = (
    base
    .filter(F.col("log_ts") > F.lit("2025-06-01").cast("date"))
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "status_code",
        "latency_ms_adjusted",
        (F.datediff(F.current_date(), "log_ts") / 365).alias("years_since_log_ts"),
    )
)

cleaned_app_logs = (
    cleaned
    .filter(F.col("level").isin(["WARN", "ERROR"]))
    .withColumn("status_code_adjusted", F.round((F.col("status_code") * 0.9), 2))
)

result = (
    cleaned_app_logs
    .select("level", "status_code_adjusted")
    .orderBy(F.col("level").desc())
)
