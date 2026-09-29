from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.year("log_ts"), "k")
w2 = Window.partitionBy("level", "k").orderBy("latency_ms")
w3 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w_ordered = Window.orderBy(F.col("latency_ms_percent_rank").desc())
w5 = Window.orderBy("status_code")
w6 = Window.partitionBy(F.year("log_ts_date"))
w7 = Window.orderBy(F.col("log_ts_year_min_latency_ms_sum_2h_times_log_ts_year_min_latency_ms").desc())
w8 = Window.orderBy(F.col("latency_ms_percent_rank_rank").desc())

lines = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "v"),
    )
)

scored = (
    lines
    .filter(F.to_date("log_ts") < "2023-03-01")
    .select(
        "log_ts",
        "status_code",
        "v",
        F.min("latency_ms").over(w).alias("log_ts_year_min_latency_ms"),
        F.percent_rank().over(w2).alias("latency_ms_percent_rank"),
    )
)

valid = (
    scored
    .filter(F.col("log_ts") >= F.lit("2025-06-01 07:00:00").cast("timestamp"))
    .select(
        "log_ts",
        "status_code",
        "v",
        "log_ts_year_min_latency_ms",
        "latency_ms_percent_rank",
        F.to_date("log_ts").alias("log_ts_date"),
        F.coalesce("latency_ms_percent_rank", F.lit(0)).alias("latency_ms_percent_rank_filled"),
    )
)

enriched = (
    valid
    .withColumn("log_ts_year_min_latency_ms_sum_2h", F.sum("log_ts_year_min_latency_ms").over(w3))
    .withColumn("latency_ms_percent_rank_rank", F.rank().over(w_ordered))
    .withColumn("status_code_percent_rank", F.percent_rank().over(w5))
    .withColumn("status_code_share", F.round((F.col("status_code") / F.sum("status_code").over(w6)), 4))
)

valid_app_logs = (
    enriched
    .filter(F.col("status_code") != 277)
    .select(
        "log_ts",
        "status_code",
        "v",
        "log_ts_year_min_latency_ms",
        "latency_ms_percent_rank",
        "log_ts_date",
        "latency_ms_percent_rank_filled",
        "log_ts_year_min_latency_ms_sum_2h",
        "latency_ms_percent_rank_rank",
        "status_code_share",
        F.coalesce("status_code_share", F.lit(0)).alias("status_code_share_filled"),
        (F.col("log_ts_year_min_latency_ms_sum_2h") * F.col("log_ts_year_min_latency_ms")).alias("log_ts_year_min_latency_ms_sum_2h_times_log_ts_year_min_latency_ms"),
    )
)

enriched_app_logs = (
    valid_app_logs
    .select(
        "log_ts",
        "status_code",
        "v",
        "log_ts_year_min_latency_ms",
        "log_ts_date",
        "latency_ms_percent_rank_filled",
        "log_ts_year_min_latency_ms_sum_2h",
        "latency_ms_percent_rank_rank",
        "status_code_share",
        "log_ts_year_min_latency_ms_sum_2h_times_log_ts_year_min_latency_ms",
        F.rank().over(w7).alias("log_ts_year_min_latency_ms_sum_2h_times_log_ts_year_min_latency_ms_rank"),
        F.sum("latency_ms_percent_rank_filled").over(w3).alias("latency_ms_percent_rank_filled_sum_2h"),
        F.rank().over(w8).alias("latency_ms_percent_rank_rank_rank"),
    )
)

result = (
    enriched_app_logs
    .withColumn("hour_window", F.date_trunc("HOUR", "log_ts"))
    .withColumn("activity_day", F.date_trunc("DAY", "log_ts"))
    .withColumn("quarter_start", F.date_trunc("QUARTER", "log_ts_date"))
    .cube("hour_window", "activity_day", "quarter_start")
    .agg(
        F.min("status_code").alias("min_status_code"),
        F.bit_or("status_code").alias("status_code_bit_or"),
        F.round(F.avg("log_ts_year_min_latency_ms_sum_2h_times_log_ts_year_min_latency_ms_rank"), 2).alias("avg_log_ts_year_min_latency_ms_sum_2h_times_log_ts_year_min_latency_ms_rank"),
        F.avg("log_ts_year_min_latency_ms_sum_2h").alias("avg_log_ts_year_min_latency_ms_sum_2h"),
        F.percentile_approx("log_ts_year_min_latency_ms_sum_2h", 0.5).alias("log_ts_year_min_latency_ms_sum_2h_p50"),
        F.round(F.avg("latency_ms_percent_rank_rank"), 2).alias("avg_latency_ms_percent_rank_rank"),
        F.grouping_id().alias("grouping_id"),
    )
)
