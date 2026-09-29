from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w_part = Window.partitionBy("attr_key").orderBy(F.col("status_code").desc())
w_part_2 = Window.partitionBy("level")
w4 = Window.partitionBy("service")
w5 = Window.partitionBy(F.to_date("log_ts"), "attr_key").orderBy(F.col("level_max_latency_ms").asc_nulls_last())
w6 = Window.orderBy(F.col("status_code").desc())
w7 = Window.orderBy(F.col("level_max_latency_ms_percent_rank").desc_nulls_first())
w8 = Window.orderBy(F.col("status_code_cume_dist").desc())
w9 = Window.partitionBy("service").orderBy(F.col("latency_ms").asc_nulls_first())

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "value"),
    )
)

enriched = (
    exploded
    .filter(F.col("latency_ms") <= 790)
    .withColumn("status_code_sum_15m", F.sum("status_code").over(w))
    .withColumn("status_code_rank", F.rank().over(w_part))
    .withColumn("level_max_latency_ms", F.max("latency_ms").over(w_part_2))
    .withColumn("status_code_share", F.round((F.col("status_code") / F.sum("status_code").over(w4)), 4))
    .withColumn("corr_status_code_latency_ms", F.corr("status_code", "latency_ms").over(w4))
)

matched_keys = (
    enriched
    .select("id")
    .intersectAll(
        df_app_logs
        .filter(F.col("status_code") == 441)
        .select("id")
    )
)

reconciled = (
    enriched
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

with_windows = (
    reconciled
    .select(
        "service",
        "level",
        "latency_ms",
        "status_code",
        "attr_key",
        "value",
        "status_code_sum_15m",
        "status_code_share",
        "corr_status_code_latency_ms",
        F.percent_rank().over(w5).alias("level_max_latency_ms_percent_rank"),
    )
)

scored = (
    with_windows
    .select(
        "service",
        "level",
        "latency_ms",
        "status_code",
        "status_code_sum_15m",
        "status_code_share",
        "corr_status_code_latency_ms",
        "level_max_latency_ms_percent_rank",
        F.cume_dist().over(w6).alias("status_code_cume_dist"),
        F.dense_rank().over(w7).alias("level_max_latency_ms_percent_rank_dense_rank"),
    )
)

cleaned = (
    scored
    .filter(~F.col("level").isin(["ERROR", "DEBUG"]))
    .withColumn("corr_status_code_latency_ms_times_status_code_share", (F.col("corr_status_code_latency_ms") * F.col("status_code_share")))
)

enriched_app_logs = (
    cleaned
    .select(
        "service",
        "latency_ms",
        "status_code_sum_15m",
        "status_code_share",
        "level_max_latency_ms_percent_rank",
        "level_max_latency_ms_percent_rank_dense_rank",
        F.cume_dist().over(w8).alias("status_code_cume_dist_cume_dist"),
        F.max("status_code").over(w_part_2).alias("level_max_status_code"),
        F.dense_rank().over(w9).alias("latency_ms_dense_rank"),
    )
)

daily = (
    enriched_app_logs
    .groupBy(
        F.when(F.col("level_max_latency_ms_percent_rank_dense_rank") >= 7, "HIGH").otherwise("LOW").alias("level_max_latency_ms_percent_rank_dense_rank_tier"),
        "service",
    )
    .agg(
        F.sum(F.when(F.col("service") == "auth", F.col("status_code_sum_15m")).otherwise(0)).alias("auth_status_code_sum_15m"),
        F.round(F.sum("status_code_sum_15m") / F.nullif(F.count(F.when(F.col("service") == "checkout", 1)), F.lit(0)), 2).alias("status_code_sum_15m_per_checkout"),
        F.count(F.when(F.col("service") == "billing", 1)).alias("billing_count"),
        F.bit_or("latency_ms").alias("latency_ms_bit_or"),
        F.sum(F.when(F.col("service") == "checkout", F.col("latency_ms_dense_rank")).otherwise(0)).alias("checkout_latency_ms_dense_rank"),
    )
    .filter(F.col("latency_ms_bit_or") > 1)
)

result = (
    daily
    .select(
        "level_max_latency_ms_percent_rank_dense_rank_tier",
        "service",
        "status_code_sum_15m_per_checkout",
        "latency_ms_bit_or",
    )
    .orderBy(
        F.col("status_code_sum_15m_per_checkout").desc(),
        F.col("latency_ms_bit_or").desc(),
    )
)
