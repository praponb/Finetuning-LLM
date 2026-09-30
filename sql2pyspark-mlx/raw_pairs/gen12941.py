from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k")
w2 = Window.partitionBy("k", "level")
w3 = Window.partitionBy(F.to_date("log_ts"), "service")
w4 = Window.partitionBy("level")
w5 = Window.orderBy(F.col("k_avg_latency_ms").desc_nulls_last())

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

metrics = (
    flattened
    .withColumn("k_avg_latency_ms", F.avg("latency_ms").over(w))
    .withColumn("k_avg_latency_ms_2", F.avg("latency_ms").over(w2))
    .withColumn("latency_ms_share", F.round((F.col("latency_ms") / F.sum("latency_ms").over(w3)), 4))
)

filtered = (
    metrics
    .filter((F.col("log_ts") >= F.lit("2025-06-01 05:00:00").cast("timestamp")) & F.col("host").isin(["h2", "h1"]))
    .select(
        "id",
        "host",
        "level",
        "latency_ms",
        "k",
        "k_avg_latency_ms",
        "k_avg_latency_ms_2",
        "latency_ms_share",
        F.concat("k", F.lit(" - "), "level").alias("k_level"),
        (F.col("k_avg_latency_ms_2") - F.col("status_code")).alias("k_avg_latency_ms_2_minus_status_code"),
        F.coalesce("k", F.lit("OTHER")).alias("resolved_k"),
    )
)

valid = (
    filtered
    .filter((F.col("k").eqNullSafe(F.lit("version"))) & F.col("host").isin(["h2", "h3", "h4"]))
    .select(
        "host",
        "level",
        "k",
        "k_avg_latency_ms",
        "k_avg_latency_ms_2",
        "latency_ms_share",
        "k_level",
        "k_avg_latency_ms_2_minus_status_code",
        "resolved_k",
        F.round((F.col("k_avg_latency_ms_2_minus_status_code") / F.col("k_avg_latency_ms_2")), 2).alias("k_avg_latency_ms_2_minus_status_code_per_k_avg_latency_ms_2"),
        F.greatest("k_avg_latency_ms", "latency_ms").alias("greatest_k_avg_latency_ms_latency_ms"),
    )
)

with_windows = (
    valid
    .filter(F.col("k_avg_latency_ms_2_minus_status_code_per_k_avg_latency_ms_2") >= 79)
    .select(
        "level",
        "k",
        "k_avg_latency_ms",
        "latency_ms_share",
        "k_level",
        "k_avg_latency_ms_2_minus_status_code",
        "resolved_k",
        "k_avg_latency_ms_2_minus_status_code_per_k_avg_latency_ms_2",
        "greatest_k_avg_latency_ms_latency_ms",
        F.avg("latency_ms_share").over(w4).alias("level_avg_latency_ms_share"),
        F.cume_dist().over(w5).alias("k_avg_latency_ms_cume_dist"),
        F.min("k_avg_latency_ms_2_minus_status_code").over(w).alias("k_min_k_avg_latency_ms_2_minus_status_code"),
    )
)

remaining_keys = (
    with_windows
    .select("level")
    .intersectAll(
        df_app_logs
        .filter(F.col("service") == "checkout")
        .select("level")
    )
)

kept_rows = (
    with_windows
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.level") == F.col("k.level")), "inner")
    .select("s.*")
)

remaining_keys_2 = (
    kept_rows
    .select("level")
    .exceptAll(
        df_app_logs
        .filter(~F.col("service").isin(["search", "checkout"]))
        .select("level")
    )
)

verified = (
    kept_rows
    .alias("s")
    .join(remaining_keys_2.alias("k"), (F.col("s.level") == F.col("k.level")), "inner")
    .select("s.*")
)

result = (
    verified
    .select(
        "level",
        "k",
        "latency_ms_share",
        "k_avg_latency_ms_2_minus_status_code",
        "resolved_k",
        "k_avg_latency_ms_2_minus_status_code_per_k_avg_latency_ms_2",
        "level_avg_latency_ms_share",
        "k_avg_latency_ms_cume_dist",
        "k_min_k_avg_latency_ms_2_minus_status_code",
    )
    .orderBy(F.col("k_avg_latency_ms_2_minus_status_code").desc())
)
