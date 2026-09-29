from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k")
w2 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w3 = Window.partitionBy("k").orderBy(F.col("status_code").desc())
w4 = Window.partitionBy("service").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w5 = Window.partitionBy("host", "k").orderBy(F.col("status_code_cume_dist").desc())
w6 = Window.partitionBy("host")
w7 = Window.partitionBy("service")
w8 = Window.partitionBy(F.to_date("log_ts"), "host").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w9 = Window.partitionBy(F.to_date("log_ts")).orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w10 = Window.partitionBy("host").orderBy(F.col("status_code_sum_30m").desc())
w11 = Window.partitionBy(F.date_trunc("WEEK", "log_ts"), "k").orderBy(F.col("status_code_cume_dist").desc())
w12 = Window.partitionBy("k").orderBy("status_code_sum_30m_dense_rank")
w13 = Window.partitionBy("k").orderBy(F.col("status_code_sum_30m_dense_rank").desc_nulls_first())
w14 = Window.partitionBy("service", "k").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "status_code",
        F.explode_outer("context").alias("k", "v"),
    )
)

metrics = (
    flattened
    .select(
        "*",
        F.count("status_code").over(w).alias("k_count_status_code"),
        F.sum("status_code").over(w2).alias("status_code_sum_30m"),
        F.cume_dist().over(w3).alias("status_code_cume_dist"),
    )
)

base = (
    metrics
    .filter(F.col("log_ts") > F.lit("2025-06-01").cast("date"))
    .withColumn("log_ts_date", F.to_date("log_ts"))
    .withColumn("k_count_status_code_band", F.when(F.col("k_count_status_code") >= 14, "High").when(F.col("k_count_status_code") >= 3, "Medium").otherwise("Low"))
)

metrics_app_logs = (
    base
    .withColumn("status_code_sum_30m_max_30m", F.max("status_code_sum_30m").over(w4))
    .withColumn("status_code_cume_dist_percent_rank", F.percent_rank().over(w5))
    .withColumn("host_sum_status_code", F.sum("status_code").over(w6))
    .withColumn("k_count_status_code_share", F.round((F.col("k_count_status_code") / F.sum("k_count_status_code").over(w7)), 4))
    .withColumn("status_code_sum_30m_count_30m", F.count("status_code_sum_30m").over(w8))
)

with_windows = (
    metrics_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "k",
        "status_code_sum_30m",
        "k_count_status_code_band",
        "status_code_sum_30m_max_30m",
        "status_code_cume_dist_percent_rank",
        "host_sum_status_code",
        "status_code_sum_30m_count_30m",
        F.avg("status_code_cume_dist_percent_rank").over(w9).alias("status_code_cume_dist_percent_rank_avg_1h"),
        F.rank().over(w3).alias("status_code_rank"),
        F.dense_rank().over(w10).alias("status_code_sum_30m_dense_rank"),
        F.percent_rank().over(w11).alias("status_code_cume_dist_percent_rank_2"),
    )
)

result = (
    with_windows
    .select(
        "log_ts",
        "host_sum_status_code",
        "status_code_sum_30m_count_30m",
        F.dense_rank().over(w12).alias("status_code_sum_30m_dense_rank_dense_rank"),
        F.dense_rank().over(w13).alias("status_code_sum_30m_dense_rank_dense_rank_2"),
        F.count("status_code_sum_30m_count_30m").over(w14).alias("status_code_sum_30m_count_30m_count_2h"),
    )
)
