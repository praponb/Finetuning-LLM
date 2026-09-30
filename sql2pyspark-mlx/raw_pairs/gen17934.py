from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host", "level").orderBy("status_code")
w2 = Window.partitionBy("level").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w_recent = Window.partitionBy("k", "level").orderBy("latency_ms", "id", "k").rowsBetween(-5, -1)
w_ordered = Window.partitionBy("host").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w5 = Window.partitionBy("host").orderBy(F.col("latency_ms_avg_5_prior").asc_nulls_last())
w_grp = Window.partitionBy("host")
w7 = Window.orderBy(F.col("latency_ms").desc())
w8 = Window.orderBy(F.col("latency_ms_rank").desc_nulls_last())
w9 = Window.orderBy(F.col("status_code_rank").asc_nulls_first())
w10 = Window.orderBy(F.col("latency_ms_rank").desc_nulls_first())

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("k", "v"),
    )
)

with_windows = (
    lines
    .select(
        "*",
        F.rank().over(w).alias("status_code_rank"),
        F.sum("status_code").over(w2).alias("status_code_sum_2h"),
        F.avg("latency_ms").over(w_recent).alias("latency_ms_avg_5_prior"),
        F.count("latency_ms").over(w_ordered).alias("latency_ms_count_15m"),
    )
)

kept_keys = (
    with_windows
    .select("level", "id")
    .intersect(
        df_app_logs
        .filter(F.col("latency_ms").between(1376, 1953))
        .select("level", "id")
    )
)

reconciled = (
    with_windows
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.level") == F.col("k.level")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

base = (
    reconciled
    .filter(F.col("k").isNotNull() & F.col("status_code").between(312, 416))
    .select(
        "id",
        "host",
        "latency_ms",
        "status_code",
        "v",
        "status_code_rank",
        "status_code_sum_2h",
        "latency_ms_avg_5_prior",
        "latency_ms_count_15m",
        F.when(F.col("status_code_rank") >= 9, "High").when(F.col("status_code_rank") >= 3, "Medium").otherwise("Low").alias("status_code_rank_level"),
        F.coalesce("k", F.lit("UNKNOWN")).alias("k_filled"),
    )
)

enriched = (
    base
    .filter(F.col("status_code_rank") >= 3)
    .select(
        "id",
        "latency_ms",
        "status_code_rank",
        "status_code_rank_level",
        F.cume_dist().over(w5).alias("latency_ms_avg_5_prior_cume_dist"),
        F.max("latency_ms").over(w_grp).alias("host_max_latency_ms"),
        F.max("status_code_rank").over(w_grp).alias("host_max_status_code_rank"),
        F.round((F.col("status_code_rank") / F.sum("status_code_rank").over(w_grp)), 4).alias("status_code_rank_share"),
        F.rank().over(w7).alias("latency_ms_rank"),
    )
)

result = (
    enriched
    .select(
        "latency_ms",
        "status_code_rank",
        "status_code_rank_level",
        "latency_ms_avg_5_prior_cume_dist",
        "host_max_status_code_rank",
        "status_code_rank_share",
        "latency_ms_rank",
        F.dense_rank().over(w8).alias("latency_ms_rank_dense_rank"),
        F.percent_rank().over(w9).alias("status_code_rank_percent_rank"),
        F.rank().over(w10).alias("latency_ms_rank_rank"),
    )
)
