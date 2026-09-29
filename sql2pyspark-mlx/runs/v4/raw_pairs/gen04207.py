from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w2 = Window.partitionBy("k").orderBy(F.col("status_code").desc())
w3 = Window.partitionBy("level", "service").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w4 = Window.partitionBy("level", "k").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w5 = Window.partitionBy("level").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w6 = Window.partitionBy("service")
w7 = Window.partitionBy("level")
w8 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w9 = Window.orderBy(F.col("status_code_max_15m_sum_1h").desc())

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "status_code",
        F.explode_outer("context").alias("k", "value"),
    )
)

with_windows = (
    lines
    .select(
        "*",
        F.max("status_code").over(w).alias("status_code_max_2h"),
        F.dense_rank().over(w2).alias("status_code_dense_rank"),
        F.max("status_code").over(w3).alias("status_code_max_15m"),
    )
)

metrics = (
    with_windows
    .select(
        "*",
        F.avg("status_code_dense_rank").over(w4).alias("status_code_dense_rank_avg_1h"),
        F.sum("status_code_max_15m").over(w5).alias("status_code_max_15m_sum_1h"),
        F.stddev_samp("status_code_max_2h").over(w6).alias("service_stddev_samp_status_code_max_2h"),
        F.avg("status_code").over(w7).alias("level_avg_status_code"),
    )
)

with_windows_app_logs = (
    metrics
    .withColumn("level_avg_status_code_max_15m", F.avg("status_code_max_15m").over(w7))
    .withColumn("status_code_max_15m_count_30m", F.count("status_code_max_15m").over(w8))
    .withColumn("status_code_max_15m_sum_1h_percent_rank", F.percent_rank().over(w9))
)

valid = (
    with_windows_app_logs
    .filter(F.col("k").isNotNull())
    .select(
        "*",
        (F.col("status_code_max_15m") * F.col("status_code_max_15m_sum_1h")).alias("status_code_max_15m_times_status_code_max_15m_sum_1h"),
        F.floor("service_stddev_samp_status_code_max_2h").alias("service_stddev_samp_status_code_max_2h_whole"),
    )
)

valid_app_logs = (
    valid
    .filter(F.col("log_ts") > F.lit("2025-06-01").cast("date"))
    .select(
        "*",
        F.round((F.col("status_code_dense_rank_avg_1h") / F.col("status_code_max_15m")), 2).alias("status_code_dense_rank_avg_1h_per_status_code_max_15m"),
        F.date_format("log_ts", "MMM yyyy").alias("log_ts_label"),
    )
)

result = (
    valid_app_logs
    .select(
        "id",
        "log_ts",
        "k",
        "value",
        "status_code_dense_rank",
        "status_code_dense_rank_avg_1h",
        "status_code_max_15m_sum_1h",
        "service_stddev_samp_status_code_max_2h",
        "level_avg_status_code",
    )
)
