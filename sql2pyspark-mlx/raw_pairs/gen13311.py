from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level")
w2 = Window.partitionBy("service")
w3 = Window.partitionBy("k", "level")

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "value"),
    )
)

with_windows = (
    lines
    .select(
        "id",
        "service",
        "value",
        F.stddev_samp("latency_ms").over(w).alias("level_stddev_samp_latency_ms"),
        F.round((F.col("latency_ms") / F.sum("latency_ms").over(w2)), 4).alias("latency_ms_share"),
        F.corr("latency_ms", "status_code").over(w3).alias("corr_latency_ms_status_code"),
    )
)

remaining_keys = (
    with_windows
    .select("id", "service")
    .intersectAll(
        df_app_logs
        .filter(F.col("host") == "h1")
        .select("id", "service")
    )
)

verified = (
    with_windows
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.service") == F.col("k.service")), "inner")
    .select("s.*")
)

cleaned = (
    verified
    .filter(F.col("service") == "billing")
    .select(
        "id",
        "level_stddev_samp_latency_ms",
        "corr_latency_ms_status_code",
        (F.col("corr_latency_ms_status_code") * F.col("latency_ms_share")).alias("corr_latency_ms_status_code_times_latency_ms_share"),
    )
)

grouped = (
    cleaned
    .groupBy(
        F.when(F.col("corr_latency_ms_status_code") >= 0.69, "PREMIUM").otherwise("STANDARD").alias("corr_latency_ms_status_code_tier"),
    )
    .agg(
        F.percentile_approx("level_stddev_samp_latency_ms", [0.25, 0.5, 0.75]).alias("level_stddev_samp_latency_ms_quartiles"),
        F.round(F.avg("level_stddev_samp_latency_ms"), 2).alias("avg_level_stddev_samp_latency_ms"),
        F.percentile_approx("corr_latency_ms_status_code_times_latency_ms_share", [0.25, 0.5, 0.75]).alias("corr_latency_ms_status_code_times_latency_ms_share_quartiles"),
        F.count("*").alias("row_count"),
        F.percentile_approx("corr_latency_ms_status_code", 0.95).alias("corr_latency_ms_status_code_p95"),
    )
)

result = (
    grouped
    .select(
        "corr_latency_ms_status_code_tier",
        "level_stddev_samp_latency_ms_quartiles",
        "row_count",
        "corr_latency_ms_status_code_p95",
    )
    .orderBy(F.col("row_count").desc_nulls_first())
)
