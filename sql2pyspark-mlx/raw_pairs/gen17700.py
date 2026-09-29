from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k")
w2 = Window.orderBy(F.col("latency_ms").desc())
w3 = Window.partitionBy("service").orderBy(F.col("latency_ms").desc_nulls_last())
w_grp = Window.partitionBy(F.year("log_ts")).orderBy("status_code")
w5 = Window.partitionBy(F.month("log_ts"), "k")
w6 = Window.partitionBy("k").orderBy(F.col("latency_ms_percent_rank").desc_nulls_last())

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "v"),
    )
)

metrics = (
    flattened
    .select(
        "id",
        "log_ts",
        "latency_ms",
        "k",
        F.avg("latency_ms").over(w).alias("k_avg_latency_ms"),
        F.percent_rank().over(w2).alias("latency_ms_percent_rank"),
        F.dense_rank().over(w3).alias("latency_ms_dense_rank"),
        F.dense_rank().over(w_grp).alias("status_code_dense_rank"),
    )
)

scored = (
    metrics
    .filter(F.col("log_ts") >= "2024-06-01")
    .select(
        "k_avg_latency_ms",
        "latency_ms_percent_rank",
        F.count("latency_ms_percent_rank").over(w5).alias("log_ts_month_count_latency_ms_percent_rank"),
        F.dense_rank().over(w6).alias("latency_ms_percent_rank_dense_rank"),
    )
)

valid = (
    scored
    .filter(F.col("latency_ms_percent_rank_dense_rank") == 6)
    .withColumn("latency_ms_percent_rank_filled", F.coalesce("latency_ms_percent_rank", F.lit(0)))
    .withColumn("log_ts_month_count_latency_ms_percent_rank_rounded", F.round("log_ts_month_count_latency_ms_percent_rank", 2))
)

filtered = (
    valid
    .filter(F.col("log_ts_month_count_latency_ms_percent_rank") != 9)
    .select(
        "*",
        (F.col("latency_ms_percent_rank") + F.col("latency_ms_percent_rank_dense_rank")).alias("latency_ms_percent_rank_plus_latency_ms_percent_rank_dense_rank"),
        (F.col("latency_ms_percent_rank_dense_rank") - F.col("latency_ms_percent_rank")).alias("latency_ms_percent_rank_dense_rank_minus_latency_ms_percent_rank"),
    )
)

result = (
    filtered
    .groupBy(
        F.when(F.col("log_ts_month_count_latency_ms_percent_rank") >= 18, "HIGH").otherwise("LOW").alias("log_ts_month_count_latency_ms_percent_rank_tier"),
        F.when(F.col("latency_ms_percent_rank_dense_rank_minus_latency_ms_percent_rank") >= 88, "PREMIUM").otherwise("STANDARD").alias("latency_ms_percent_rank_dense_rank_minus_latency_ms_percent_rank_tier"),
        F.when(F.col("latency_ms_percent_rank_plus_latency_ms_percent_rank_dense_rank") >= 73, "LARGE").otherwise("SMALL").alias("latency_ms_percent_rank_plus_latency_ms_percent_rank_dense_rank_tier"),
    )
    .agg(
        F.percentile_approx("latency_ms_percent_rank_plus_latency_ms_percent_rank_dense_rank", [0.25, 0.5, 0.75]).alias("latency_ms_percent_rank_plus_latency_ms_percent_rank_dense_rank_quartiles"),
        F.percentile_approx("latency_ms_percent_rank_dense_rank_minus_latency_ms_percent_rank", [0.25, 0.5, 0.75]).alias("latency_ms_percent_rank_dense_rank_minus_latency_ms_percent_rank_quartiles"),
    )
    .orderBy("log_ts_month_count_latency_ms_percent_rank_tier")
)
