from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k", "level").orderBy("status_code", "id", "k")
w2 = Window.orderBy("status_code", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("host", "k").orderBy(F.col("log_ts_hour").desc())
w4 = Window.partitionBy("k").orderBy("status_code", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("level").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w_ordered = Window.partitionBy("level", "k")
w_part = Window.orderBy("status_code")
w_recent = Window.partitionBy("level").orderBy(F.col("log_ts_hour").asc_nulls_last())
w_grp = Window.partitionBy("host", "level").orderBy(F.col("log_ts_hour").desc(), "id", "k")
w10 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)

lines = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "status_code",
        F.explode("context").alias("k", "value"),
    )
)

base = (
    lines
    .filter(F.col("k").isNotNull())
    .withColumn("log_ts_hour", F.hour("log_ts"))
    .withColumn("k_clean", F.coalesce("k", F.lit("DIRECT")))
)

scored = (
    base
    .withColumn("seq_no", F.row_number().over(w))
    .withColumn("cumulative_status_code", F.sum("status_code").over(w2))
    .withColumn("log_ts_hour_percent_rank", F.percent_rank().over(w3))
    .withColumn("first_known_log_ts_hour", F.first("log_ts_hour", ignorenulls=True).over(w4))
    .withColumn("log_ts_hour_count_1h", F.count("log_ts_hour").over(w5))
)

metrics = (
    scored
    .filter(F.col("log_ts_hour_count_1h") > 12)
    .withColumn("level_stddev_samp_first_known_log_ts_hour", F.stddev_samp("first_known_log_ts_hour").over(w_ordered))
    .withColumn("status_code_dense_rank", F.dense_rank().over(w_part))
    .withColumn("log_ts_hour_cume_dist", F.cume_dist().over(w_recent))
    .withColumn("row_seq", F.row_number().over(w_grp))
    .withColumn("first_known_log_ts_hour_max_30m", F.max("first_known_log_ts_hour").over(w10))
)

valid = (
    metrics
    .filter(F.col("k").isNotNull() & (~F.col("k").eqNullSafe(F.lit("region"))))
    .withColumn("log_ts_period", F.date_trunc("week", "log_ts"))
    .withColumn("k_clean_2", F.coalesce("k", F.lit("UNKNOWN")))
)

result = (
    valid
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "status_code",
        "k",
        "log_ts_hour",
        "k_clean",
        "cumulative_status_code",
        F.when(F.col("level").rlike("^(INFO|ERROR)$"), "GROUP_B").otherwise("OTHER_ZONE").alias("level_zone"),
    )
)
