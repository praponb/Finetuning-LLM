from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("attr_key")
w_hist = Window.partitionBy("attr_key", "service")
w_part = Window.partitionBy("attr_key", "service").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w_running = Window.orderBy(F.col("attr_key_sum_days_since_log_ts").desc_nulls_last())
w5 = Window.orderBy(F.col("attr_key_avg_days_since_log_ts_sum_2h").asc_nulls_first())
w6 = Window.partitionBy("service", "attr_key").orderBy(F.col("attr_key_sum_days_since_log_ts").desc_nulls_first())
w7 = Window.partitionBy("service", "attr_key").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w8 = Window.partitionBy("service").orderBy(F.col("attr_key_avg_days_since_log_ts_sum_2h_rank").desc_nulls_first())
w_grp = Window.partitionBy("service", "attr_key").orderBy(F.col("days_since_log_ts").desc_nulls_first())
w10 = Window.partitionBy("service").orderBy(F.col("attr_key_sum_attr_key_sum_days_since_log_ts").asc_nulls_first())

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        F.explode_outer("context").alias("attr_key", "attr_value"),
    )
)

filtered = (
    lines
    .filter(F.col("service").isin(["search", "checkout", "auth"]))
    .select("*", F.coalesce("attr_key", F.lit("unassigned")).alias("attr_key_filled"))
)

filtered_app_logs = (
    filtered
    .filter(F.col("attr_key").isNotNull())
    .withColumn("service_attr_key_key", F.concat_ws("-", "service", "attr_key"))
    .withColumn("days_since_log_ts", F.datediff(F.current_date(), "log_ts"))
)

matched_keys = (
    filtered_app_logs
    .select("service", "id")
    .subtract(
        df_app_logs
        .filter(F.col("status_code") < 325)
        .select("service", "id")
    )
)

reconciled = (
    filtered_app_logs
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.service") == F.col("k.service")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

with_windows = (
    reconciled
    .withColumn("attr_key_avg_days_since_log_ts", F.avg("days_since_log_ts").over(w_recent))
    .withColumn("attr_key_sum_days_since_log_ts", F.sum("days_since_log_ts").over(w_hist))
)

metrics = (
    with_windows
    .withColumn("attr_key_avg_days_since_log_ts_sum_2h", F.sum("attr_key_avg_days_since_log_ts").over(w_part))
    .withColumn("attr_key_count_attr_key_avg_days_since_log_ts", F.count("attr_key_avg_days_since_log_ts").over(w_recent))
)

enriched = (
    metrics
    .withColumn("attr_key_sum_attr_key_sum_days_since_log_ts", F.sum("attr_key_sum_days_since_log_ts").over(w_hist))
    .withColumn("attr_key_sum_days_since_log_ts_dense_rank", F.dense_rank().over(w_running))
    .withColumn("attr_key_avg_days_since_log_ts_sum_2h_rank", F.rank().over(w5))
    .withColumn("attr_key_sum_days_since_log_ts_cume_dist", F.cume_dist().over(w6))
    .withColumn("attr_key_avg_days_since_log_ts_sum_2h_count_15m", F.count("attr_key_avg_days_since_log_ts_sum_2h").over(w7))
)

result = (
    enriched
    .select(
        "*",
        F.dense_rank().over(w8).alias("attr_key_avg_days_since_log_ts_sum_2h_rank_dense_rank"),
        F.cume_dist().over(w_grp).alias("days_since_log_ts_cume_dist"),
        F.dense_rank().over(w10).alias("attr_key_sum_attr_key_sum_days_since_log_ts_dense_rank"),
    )
)
