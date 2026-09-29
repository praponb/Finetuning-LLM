from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.date_trunc("WEEK", "txn_time"), "account_id").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w4 = Window.partitionBy("account_id", "txn_type").orderBy(F.col("session_start_flag").asc_nulls_first())
w5 = Window.partitionBy(F.year("txn_time"), "txn_type").orderBy("session_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("account_id", "txn_type").orderBy("txn_time", "id")
w7 = Window.partitionBy(F.date_trunc("month", "txn_time"))
w8 = Window.orderBy("amount", "id")
w9 = Window.orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.orderBy("session_start_flag_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.orderBy("amount_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w12 = Window.partitionBy("year_start").orderBy(F.col("min_running_sum_seq_no").desc())

base = (
    df_transactions
    .filter(F.col("txn_time") > F.to_date(F.lit("2023-05-17")))
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "is_flagged",
        F.lower("txn_type").alias("txn_type_lower"),
        F.hour("txn_time").alias("txn_time_hour"),
    )
)

flagged = (
    base
    .withColumn("session_start_flag", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("session_start_flag").over(w2))

metrics = (
    sessionized
    .select(
        "id",
        "amount",
        "txn_time",
        "is_flagged",
        "txn_type_lower",
        "txn_time_hour",
        "session_start_flag",
        F.sum("txn_time_hour").over(w3).alias("txn_time_hour_sum_90d"),
        F.rank().over(w4).alias("session_start_flag_rank"),
        F.sum("txn_time_hour").over(w5).alias("cumulative_txn_time_hour"),
        F.row_number().over(w6).alias("seq_no"),
    )
)

scored = (
    metrics
    .select(
        "id",
        "amount",
        "txn_time",
        "txn_type_lower",
        "txn_time_hour_sum_90d",
        "seq_no",
        F.stddev_samp("seq_no").over(w7).alias("txn_time_period_stddev_samp_seq_no"),
        F.ntile(4).over(w8).alias("amount_bucket"),
        F.sum("session_start_flag_rank").over(w9).alias("running_session_start_flag_rank"),
        F.sum("txn_time_hour_sum_90d").over(w10).alias("running_txn_time_hour_sum_90d"),
    )
)

scored_transactions = (
    scored
    .filter(F.col("txn_time_period_stddev_samp_seq_no") < 5)
    .select(
        "*",
        F.count("seq_no").over(w9).alias("running_count_seq_no"),
        F.max("seq_no").over(w9).alias("running_max_seq_no"),
        F.min("seq_no").over(w9).alias("running_min_seq_no"),
        F.sum("seq_no").over(w9).alias("running_sum_seq_no"),
        F.first("running_txn_time_hour_sum_90d", ignorenulls=True).over(w11).alias("first_known_running_txn_time_hour_sum_90d"),
    )
)

result = (
    scored_transactions
    .groupBy(
        F.date_trunc("year", "txn_time").alias("year_start"),
        F.when(F.col("running_min_seq_no") >= 7.3, "HIGH").otherwise("LOW").alias("running_min_seq_no_tier"),
        F.date_trunc("quarter", "txn_time").alias("quarter_start"),
    )
    .agg(
        F.min("running_sum_seq_no").alias("min_running_sum_seq_no"),
        F.percentile_approx("txn_time_hour_sum_90d", [0.25, 0.5, 0.75]).alias("txn_time_hour_sum_90d_quartiles"),
    )
    .withColumn("min_running_sum_seq_no_percent_rank", F.percent_rank().over(w12))
)
