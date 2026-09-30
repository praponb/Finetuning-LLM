from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy(F.month("txn_time"), "account_id").orderBy(F.col("amount").desc_nulls_last())
w2 = Window.partitionBy("txn_type", "merchant")
w_grp = Window.partitionBy("account_id")
w4 = Window.partitionBy("txn_type", "merchant").orderBy(F.col("account_id_stddev_samp_amount_plus_amount_share").desc_nulls_last(), "id")
w5 = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w6 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("account_type").orderBy("session_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_windows = (
    df_transactions
    .withColumn("amount_dense_rank", F.dense_rank().over(w_recent))
    .withColumn("amount_share", F.round((F.col("amount") / F.sum("amount").over(w2)), 4))
    .withColumn("account_id_stddev_samp_amount", F.stddev_samp("amount").over(w_grp))
)

base = (
    with_windows
    .filter(F.col("txn_time") > F.lit("2023-07-25 05:00:00").cast("timestamp"))
    .withColumn("account_id_stddev_samp_amount_plus_amount_share", (F.col("account_id_stddev_samp_amount") + F.col("amount_share")))
    .withColumn("txn_time_period", F.date_trunc("week", "txn_time"))
)

enriched = (
    base
    .alias("s")
    .join(df_accounts.alias("p"), F.col("s.account_id") == F.col("p.id"), "left")
    .filter(F.col("p.customer_id").isin([5, 26]))
    .select("s.*", F.col("p.account_type"))
)

ranked = (
    enriched
    .withColumn("row_num", F.row_number().over(w4))
    .filter(F.col("row_num") == 1)
)

with_gaps = (
    ranked
    .withColumn("new_group_flag", F.when(F.col("txn_time") - F.lag("txn_time").over(w5) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("island_id", F.sum("new_group_flag").over(w6))

gaps = (
    sessionized
    .withColumn("new_group_flag_2", F.when(F.col("txn_time") - F.lag("txn_time").over(w5) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessionized_transactions = gaps.withColumn("session_no", F.sum("new_group_flag_2").over(w6))

with_windows_transactions = (
    sessionized_transactions
    .select(
        "*",
        F.sum("new_group_flag_2").over(w7).alias("cumulative_new_group_flag_2"),
        F.avg("account_id_stddev_samp_amount").over(w_grp).alias("account_id_avg_account_id_stddev_samp_amount"),
    )
)

result = (
    with_windows_transactions
    .alias("a")
    .join(df_accounts.alias("p"), F.col("a.account_id") == F.col("p.id"), "left")
    .rollup(
        F.col("p.account_type").alias("account_account_type"),
        F.col("a.txn_type").alias("txn_type"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.avg(F.col("a.account_id_avg_account_id_stddev_samp_amount")).alias("avg_account_id_avg_account_id_stddev_samp_amount"),
        F.max(F.col("a.amount_dense_rank")).alias("max_amount_dense_rank"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy(F.col("account_account_type").asc_nulls_last())
)
