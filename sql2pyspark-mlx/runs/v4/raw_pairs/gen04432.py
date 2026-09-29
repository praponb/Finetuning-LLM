from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("key").orderBy(F.col("status_code").desc())
w2 = Window.partitionBy("service").orderBy(F.col("status_code_dense_rank").desc_nulls_first())
w3 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w4 = Window.partitionBy("key")
w_hist = Window.orderBy(F.col("status_code").desc())
w6 = Window.partitionBy("service")

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "status_code",
        F.explode_outer("context").alias("key", "v"),
    )
)

base = (
    exploded
    .filter(~F.col("service").isin(["billing", "auth", "search"]))
    .withColumn("key_group", F.when(F.col("key").isin(["version", "region"]), "Core").otherwise("Other"))
    .withColumn("service_prefix", F.substring("service", 1, 4))
)

remaining_keys = (
    base
    .select("host", "id")
    .exceptAll(
        df_app_logs
        .filter(F.col("log_ts") < "2024-03-17")
        .select("host", "id")
    )
)

kept_rows = (
    base
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.host") == F.col("k.host")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

matched_keys = (
    kept_rows
    .select("host", "id")
    .intersectAll(
        df_app_logs
        .filter(F.col("status_code") != 353)
        .select("host", "id")
    )
)

verified = (
    kept_rows
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.host") == F.col("k.host")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

with_windows = (
    verified
    .withColumn("status_code_percent_rank", F.percent_rank().over(w_recent))
    .withColumn("status_code_dense_rank", F.dense_rank().over(w_recent))
)

with_windows_app_logs = (
    with_windows
    .withColumn("status_code_dense_rank_cume_dist", F.cume_dist().over(w2))
    .withColumn("status_code_avg_15m", F.avg("status_code").over(w3))
    .withColumn("key_stddev_samp_status_code_dense_rank", F.stddev_samp("status_code_dense_rank").over(w4))
    .withColumn("status_code_cume_dist", F.cume_dist().over(w_hist))
    .withColumn("service_min_status_code_dense_rank", F.min("status_code_dense_rank").over(w6))
)

result = (
    with_windows_app_logs
    .groupBy(
        F.when(F.col("status_code_dense_rank") >= 9, "PREMIUM").otherwise("STANDARD").alias("status_code_dense_rank_tier"),
    )
    .agg(
        F.round(F.avg("service_min_status_code_dense_rank"), 2).alias("avg_service_min_status_code_dense_rank"),
        F.count(F.when(F.col("service") == "billing", 1)).alias("billing_count"),
        F.min("log_ts").alias("started_at"),
        F.round(F.sum("status_code_avg_15m") / F.nullif(F.count(F.when(F.col("key") == "trace", 1)), F.lit(0)), 2).alias("status_code_avg_15m_per_trace"),
        F.count_if(F.col("key") == "version").alias("version_events"),
    )
)
