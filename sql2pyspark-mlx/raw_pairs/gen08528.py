from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key")
w2 = Window.partitionBy(F.month("log_ts"), "key").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w3 = Window.partitionBy("host")

lines = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("key", "attr_value"),
    )
)

kept_keys = (
    lines
    .select("id")
    .subtract(
        df_app_logs
        .filter(F.col("host").isin(["h1", "h4"]))
        .select("id")
    )
)

verified = (
    lines
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

metrics = (
    verified
    .select(
        "id",
        "host",
        "latency_ms",
        F.min("latency_ms").over(w).alias("key_min_latency_ms"),
        F.count("latency_ms").over(w2).alias("latency_ms_count_30m"),
    )
)

with_windows = (
    metrics
    .select(
        "id",
        "host",
        F.round((F.col("latency_ms") / F.sum("latency_ms").over(w3)), 4).alias("latency_ms_share"),
        F.max("key_min_latency_ms").over(w3).alias("host_max_key_min_latency_ms"),
        F.round((F.col("key_min_latency_ms") / F.sum("key_min_latency_ms").over(w3)), 4).alias("key_min_latency_ms_share"),
        F.round((F.col("latency_ms") / F.sum("latency_ms").over(w3)), 4).alias("latency_ms_share_2"),
    )
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("latency_ms_share_2") >= 0.67, "HIGH").otherwise("LOW").alias("latency_ms_share_2_tier"),
        "host",
    )
    .agg(
        F.sum(F.when(F.col("host") == "h1", F.col("latency_ms_share")).otherwise(0)).alias("h1_latency_ms_share"),
        F.max(F.when(F.col("host") == "h3", F.col("latency_ms_share"))).alias("h3_max_latency_ms_share"),
        F.percentile_approx("latency_ms_share_2", [0.25, 0.5, 0.75]).alias("latency_ms_share_2_quartiles"),
        F.count("*").alias("event_count"),
    )
)
