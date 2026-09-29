from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key")
w2 = Window.partitionBy("attr_key").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w3 = Window.partitionBy("host")
w4 = Window.orderBy(F.col("has_fault").asc_nulls_first())
w5 = Window.partitionBy("attr_key").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w6 = Window.partitionBy("host").orderBy(F.col("has_fault").desc_nulls_last())
w7 = Window.partitionBy("host", "attr_key").orderBy(F.col("status_code_high").desc_nulls_last())
w8 = Window.partitionBy("host", "attr_key").orderBy(F.col("status_code_high_avg_30m").desc_nulls_first())
w9 = Window.partitionBy("host", "attr_key")
w_recent_2 = Window.partitionBy("attr_key").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)

lines = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "status_code",
        F.explode_outer("context").alias("attr_key", "attr_value"),
    )
)

parsed = (
    lines
    .select(
        "host",
        "log_ts",
        "attr_key",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("has_fault"),
    )
)

filtered = (
    parsed
    .filter((F.col("log_ts") > F.lit("2025-06-01 13:00:00").cast("timestamp")) & F.col("attr_key").isNotNull())
    .withColumn("status_code_high_band", F.when(F.col("status_code_high") >= 2, "High").when(F.col("status_code_high") >= 1, "Medium").otherwise("Low"))
)

enriched = (
    filtered
    .select(
        "*",
        F.count("status_code_high").over(w).alias("attr_key_count_status_code_high"),
        F.sum("has_fault").over(w2).alias("has_fault_sum_2h"),
        F.sum("status_code_high").over(w3).alias("host_sum_status_code_high"),
        F.percent_rank().over(w4).alias("has_fault_percent_rank"),
        F.avg("status_code_high").over(w5).alias("status_code_high_avg_30m"),
    )
)

scored = (
    enriched
    .withColumn("has_fault_dense_rank", F.dense_rank().over(w6))
    .withColumn("status_code_high_dense_rank", F.dense_rank().over(w7))
    .withColumn("status_code_high_avg_30m_rank", F.rank().over(w8))
)

with_windows = (
    scored
    .withColumn("status_code_high_dense_rank_share", F.round((F.col("status_code_high_dense_rank") / F.sum("status_code_high_dense_rank").over(w)), 4))
    .withColumn("host_count_has_fault_sum_2h", F.count("has_fault_sum_2h").over(w3))
    .withColumn("status_code_high_avg_30m_rank_share", F.round((F.col("status_code_high_avg_30m_rank") / F.sum("status_code_high_avg_30m_rank").over(w9)), 4))
    .withColumn("has_fault_dense_rank_max_15m", F.max("has_fault_dense_rank").over(w_recent_2))
    .withColumn("host_stddev_samp_has_fault_percent_rank", F.stddev_samp("has_fault_percent_rank").over(w3))
)

result = (
    with_windows
    .groupBy("attr_key")
    .agg(
        F.count_if(F.col("host") == "h3").alias("h3_events"),
        F.percentile_approx("has_fault_sum_2h", [0.25, 0.5, 0.75]).alias("has_fault_sum_2h_quartiles"),
        F.count(F.when(F.col("host") == "h1", 1)).alias("h1_count"),
        F.sort_array(F.collect_set("host")).alias("host_list"),
        F.min("log_ts").alias("started_at"),
        F.count_if(F.col("attr_key") == "region").alias("region_events"),
    )
)
