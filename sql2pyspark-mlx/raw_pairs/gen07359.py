from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("log_ts"), "host")
w2 = Window.partitionBy("k").orderBy(F.col("log_ts").desc(), "id", "k")
w3 = Window.partitionBy("host").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w4 = Window.partitionBy("k")
w5 = Window.partitionBy("k").orderBy("status_code_max_1h", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("k").orderBy("last_known_log_ts_month_stddev_samp_status_code", "id", "k")
w7 = Window.partitionBy("k").orderBy("last_known_log_ts_month_stddev_samp_status_code", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("k").orderBy("status_code_max_1h_share", "id", "k")
w9 = Window.partitionBy("k").orderBy(F.col("next_status_code_max_1h_share").desc(), "id", "k")

exploded = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "status_code",
        F.explode("context").alias("k", "v"),
    )
)

metrics = (
    exploded
    .select(
        "id",
        "k",
        F.stddev_samp("status_code").over(w).alias("log_ts_month_stddev_samp_status_code"),
        F.row_number().over(w2).alias("row_seq"),
        F.max("status_code").over(w3).alias("status_code_max_1h"),
    )
)

with_windows = (
    metrics
    .filter(F.col("row_seq").isNotNull())
    .select(
        "id",
        "k",
        "log_ts_month_stddev_samp_status_code",
        "status_code_max_1h",
        F.round((F.col("status_code_max_1h") / F.sum("status_code_max_1h").over(w4)), 4).alias("status_code_max_1h_share"),
        F.last("log_ts_month_stddev_samp_status_code", ignorenulls=True).over(w5).alias("last_known_log_ts_month_stddev_samp_status_code"),
    )
)

metrics_app_logs = (
    with_windows
    .select(
        "id",
        "k",
        "status_code_max_1h_share",
        "last_known_log_ts_month_stddev_samp_status_code",
        F.lag("log_ts_month_stddev_samp_status_code", 1).over(w6).alias("prev_log_ts_month_stddev_samp_status_code"),
        F.last("last_known_log_ts_month_stddev_samp_status_code", ignorenulls=True).over(w7).alias("last_known_last_known_log_ts_month_stddev_samp_status_code"),
    )
)

with_windows_app_logs = (
    metrics_app_logs
    .select(
        "id",
        "k",
        "last_known_log_ts_month_stddev_samp_status_code",
        "last_known_last_known_log_ts_month_stddev_samp_status_code",
        F.lead("status_code_max_1h_share", 2).over(w_running).alias("next_status_code_max_1h_share"),
        F.row_number().over(w_running).alias("row_seq"),
    )
)

ranked = (
    with_windows_app_logs
    .withColumn("rank_in_group", F.row_number().over(w9))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    ranked
    .select(
        "id",
        "k",
        "last_known_last_known_log_ts_month_stddev_samp_status_code",
        "next_status_code_max_1h_share",
        "rank_in_group",
        F.when(F.col("k").rlike("^(region|trace)$"), "CORE").when(F.col("k").rlike("^(version)$"), "OTHER_ZONE").otherwise("PRIORITY").alias("k_zone"),
    )
    .orderBy("last_known_last_known_log_ts_month_stddev_samp_status_code")
)
