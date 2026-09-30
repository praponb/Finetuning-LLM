from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key")
w2 = Window.partitionBy("key").orderBy(F.col("latency_ms").asc_nulls_last())

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("key", "value"),
    )
)

valid = (
    exploded
    .filter(F.col("status_code") == 414)
    .select(
        "*",
        (F.datediff(F.current_date(), "log_ts") / 7).alias("weeks_since_log_ts"),
    )
)

remaining_keys = (
    valid
    .select("id")
    .exceptAll(
        df_app_logs
        .filter(F.col("status_code").between(289, 383))
        .select("id")
    )
)

kept_rows = (
    valid
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

filtered = (
    kept_rows
    .filter((F.col("log_ts") >= F.lit("2025-06-01").cast("date")) & (~F.col("key").eqNullSafe(F.lit("version"))))
    .select(
        "id",
        "latency_ms",
        "status_code",
        "key",
        "value",
        "weeks_since_log_ts",
        F.round((F.col("status_code") / F.col("latency_ms")), 2).alias("status_code_per_latency_ms"),
    )
)

kept_keys = (
    filtered
    .select("id")
    .intersectAll(
        df_app_logs
        .filter(F.col("level") == "WARN")
        .select("id")
    )
)

verified = (
    filtered
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

base = (
    verified
    .filter(F.col("key").isNotNull() & (F.col("weeks_since_log_ts") < 73))
    .withColumn("key_group", F.when(F.col("key").isin(["region", "trace"]), "Core").otherwise("Other"))
    .withColumn("status_code_band", F.when(F.col("status_code") >= 413, "High").when(F.col("status_code") >= 234, "Medium").otherwise("Low"))
    .withColumn("key_clean", F.coalesce("key", F.lit("NONE")))
)

metrics = (
    base
    .withColumn("corr_latency_ms_status_code", F.corr("latency_ms", "status_code").over(w))
)

result = (
    metrics
    .withColumn("key_max_status_code_per_latency_ms", F.max("status_code_per_latency_ms").over(w))
    .withColumn("key_avg_latency_ms", F.avg("latency_ms").over(w))
    .withColumn("key_min_weeks_since_log_ts", F.min("weeks_since_log_ts").over(w))
    .withColumn("key_min_status_code_per_latency_ms", F.min("status_code_per_latency_ms").over(w))
    .withColumn("latency_ms_dense_rank", F.dense_rank().over(w2))
)
