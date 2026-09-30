from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service")
w2 = Window.partitionBy("host").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w3 = Window.partitionBy("service").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w4 = Window.partitionBy(F.year("log_ts"), "k").orderBy(F.col("k_length").desc_nulls_first())
w5 = Window.partitionBy("host", "k").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w6 = Window.partitionBy("host").orderBy(F.col("service_min_k_length").desc_nulls_first())
w7 = Window.partitionBy("host").orderBy(F.col("service_min_k_length").desc())
w8 = Window.orderBy("k_length_percent_rank")
w9 = Window.partitionBy("host").orderBy(F.col("service_min_k_length_count_30m").asc_nulls_first())
w10 = Window.partitionBy("host")

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        F.explode_outer("context").alias("k", "value"),
    )
)

base = (
    lines
    .filter((F.col("log_ts") >= F.lit("2025-06-01").cast("date")) & F.col("k").isNotNull())
    .select(
        "*",
        F.initcap(F.trim("host")).alias("host_title"),
        F.length("k").alias("k_length"),
    )
)

enriched = (
    base
    .select(
        "*",
        F.min("k_length").over(w).alias("service_min_k_length"),
        F.max("k_length").over(w2).alias("k_length_max_15m"),
        F.sum("k_length").over(w3).alias("k_length_sum_1h"),
    )
)

metrics = (
    enriched
    .filter(F.col("log_ts") < "2023-05-01")
    .select(
        "id",
        "host",
        "value",
        "service_min_k_length",
        F.percent_rank().over(w4).alias("k_length_percent_rank"),
        F.count("service_min_k_length").over(w5).alias("service_min_k_length_count_30m"),
    )
)

with_windows = (
    metrics
    .select(
        "id",
        "value",
        "service_min_k_length",
        "service_min_k_length_count_30m",
        F.rank().over(w6).alias("service_min_k_length_rank"),
        F.percent_rank().over(w7).alias("service_min_k_length_percent_rank"),
        F.cume_dist().over(w8).alias("k_length_percent_rank_cume_dist"),
        F.percent_rank().over(w9).alias("service_min_k_length_count_30m_percent_rank"),
        F.count("service_min_k_length").over(w10).alias("host_count_service_min_k_length"),
    )
)

result = (
    with_windows
    .cube(
        F.when(F.col("service_min_k_length_count_30m") >= 16, "LARGE").otherwise("SMALL").alias("service_min_k_length_count_30m_tier"),
        F.when(F.col("service_min_k_length_count_30m_percent_rank") >= 0.69, "HIGH").otherwise("LOW").alias("service_min_k_length_count_30m_percent_rank_tier"),
    )
    .agg(
        F.round(F.avg("k_length_percent_rank_cume_dist"), 2).alias("avg_k_length_percent_rank_cume_dist"),
        F.count("*").alias("num_rows"),
        F.avg("host_count_service_min_k_length").alias("avg_host_count_service_min_k_length"),
        F.avg("service_min_k_length").alias("avg_service_min_k_length"),
        F.percentile_approx("service_min_k_length_count_30m_percent_rank", 0.9).alias("service_min_k_length_count_30m_percent_rank_p90"),
        F.round(F.avg("service_min_k_length"), 2).alias("avg_service_min_k_length_2"),
    )
    .filter(F.col("avg_service_min_k_length_2") > 0)
    .orderBy(F.col("service_min_k_length_count_30m_percent_rank_p90").desc())
)
