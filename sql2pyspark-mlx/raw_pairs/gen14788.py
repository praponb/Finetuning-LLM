from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host")
w2 = Window.partitionBy("host").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w3 = Window.partitionBy("host").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w4 = Window.partitionBy("host").orderBy(F.col("days_since_log_ts").desc_nulls_first())
w5 = Window.partitionBy(F.month("log_ts"), "level").orderBy(F.col("days_since_log_ts").desc_nulls_first())
w6 = Window.partitionBy("level")
w7 = Window.orderBy(F.col("days_since_log_ts_count_30m").desc())
w8 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w9 = Window.partitionBy("service", "host").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w10 = Window.partitionBy("service").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w11 = Window.partitionBy(F.to_date("log_ts"), "level").orderBy(F.col("host_max_days_since_log_ts").desc())

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "status_code",
        F.explode_outer("context").alias("key", "attr_value"),
    )
)

valid = (
    lines
    .filter(F.col("key").isNotNull())
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "status_code",
        F.when(F.col("host").isin(["h2", "h3"]), "Core").otherwise("Other").alias("host_group"),
        F.datediff(F.current_date(), "log_ts").alias("days_since_log_ts"),
    )
)

metrics = (
    valid
    .filter(F.col("host") != "h2")
    .withColumn("host_max_days_since_log_ts", F.max("days_since_log_ts").over(w))
    .withColumn("days_since_log_ts_max_1h", F.max("days_since_log_ts").over(w2))
    .withColumn("days_since_log_ts_count_30m", F.count("days_since_log_ts").over(w3))
    .withColumn("days_since_log_ts_rank", F.rank().over(w4))
    .withColumn("days_since_log_ts_rank_2", F.rank().over(w5))
)

scored = (
    metrics
    .select(
        "*",
        F.min("days_since_log_ts_rank").over(w6).alias("level_min_days_since_log_ts_rank"),
        F.percent_rank().over(w7).alias("days_since_log_ts_count_30m_percent_rank"),
        F.max("days_since_log_ts_max_1h").over(w8).alias("days_since_log_ts_max_1h_max_30m"),
        F.count("host_max_days_since_log_ts").over(w9).alias("host_max_days_since_log_ts_count_30m"),
        F.max("days_since_log_ts_rank_2").over(w8).alias("days_since_log_ts_rank_2_max_30m"),
    )
)

with_windows = (
    scored
    .select(
        "service",
        "level",
        "status_code",
        "host_group",
        "days_since_log_ts_max_1h",
        "days_since_log_ts_rank",
        "days_since_log_ts_count_30m_percent_rank",
        "days_since_log_ts_max_1h_max_30m",
        "host_max_days_since_log_ts_count_30m",
        "days_since_log_ts_rank_2_max_30m",
        F.sum("status_code").over(w10).alias("status_code_sum_30m"),
        F.stddev_samp("status_code").over(w).alias("host_stddev_samp_status_code"),
        F.cume_dist().over(w11).alias("host_max_days_since_log_ts_cume_dist"),
    )
)

result = (
    with_windows
    .groupBy("service")
    .agg(
        F.sort_array(F.collect_set("level")).alias("level_list"),
        F.bit_or("status_code").alias("status_code_bit_or"),
        F.countDistinct("level").alias("distinct_levels"),
        F.countDistinct("service").alias("distinct_services"),
    )
)
