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
        F.explode_outer("context").alias("k", "value"),
    )
)

filtered = (
    lines
    .filter((F.col("host") == "h4") & (F.col("log_ts") >= F.lit("2025-06-01").cast("date")))
    .withColumn("k_host", F.concat("k", F.lit(" - "), "host"))
)

cleaned = (
    filtered
    .filter((F.col("log_ts") >= F.lit("2025-06-01").cast("date")) & F.col("k").isNotNull())
    .select(
        "id",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        "k",
        "value",
        "k_host",
        F.round((F.col("latency_ms") / F.col("status_code")), 2).alias("latency_ms_per_status_code"),
    )
)

matched_keys = (
    cleaned
    .select("id", "host")
    .subtract(
        df_app_logs
        .filter(F.col("latency_ms") >= 2244)
        .select("id", "host")
    )
)

verified = (
    cleaned
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.host") == F.col("k.host")), "inner")
    .select("s.*")
)

base = (
    verified
    .filter(F.col("log_ts") > F.lit("2025-06-01 10:00:00").cast("timestamp"))
    .select(
        "id",
        "log_ts",
        "latency_ms",
        "status_code",
        "k",
        "value",
        "k_host",
        F.concat("k", F.lit("/"), "host").alias("k_host_2"),
        F.coalesce("k", F.lit("DIRECT")).alias("k_clean"),
    )
)

result = (
    base
    .select("log_ts", "latency_ms", "status_code", "k", "k_host", "k_host_2", "k_clean")
    .orderBy("k")
)
