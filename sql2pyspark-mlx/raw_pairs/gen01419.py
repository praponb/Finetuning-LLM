from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("account_id").orderBy(F.col("amount").desc(), "id")
w2 = Window.partitionBy("txn_type")
w_part_2 = Window.partitionBy("account_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("merchant").orderBy(F.col("amount").desc(), "id")
w5 = Window.partitionBy("merchant").orderBy("txn_time", "id")
w6 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w8 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("txn_type").orderBy("amount_bucket_2", "id").rowsBetween(-3, Window.currentRow)
w10 = Window.orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)

with_windows = (
    df_transactions
    .filter(F.col("is_flagged"))
    .select(
        "id",
        "txn_type",
        "txn_time",
        "merchant",
        "is_flagged",
        F.ntile(3).over(w_part).alias("amount_bucket"),
        F.round((F.col("amount") / F.sum("amount").over(w2)), 4).alias("amount_share"),
        F.last("merchant", ignorenulls=True).over(w_part_2).alias("last_known_merchant"),
        F.sum("amount").over(w_part_2).alias("cumulative_amount"),
        F.ntile(3).over(w4).alias("amount_bucket_2"),
    )
)

with_prev = with_windows.withColumn("prev_txn_time", F.lag("txn_time").over(w5))

gaps = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_txn_time").isNull() | (F.col("txn_time") > F.col("prev_txn_time") + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessionized = gaps.withColumn("session_no", F.sum("is_new_session").over(w6))

with_gaps = (
    sessionized
    .withColumn("new_group_flag", F.when(F.lag("txn_time").over(w7).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w7) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessionized_transactions = with_gaps.withColumn("session_id", F.sum("new_group_flag").over(w8))

with_gaps_2 = (
    sessionized_transactions
    .withColumn("session_start_flag", F.when(F.lag("txn_time").over(w5).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w5) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessionized_transactions_2 = (
    with_gaps_2
    .select(
        "id",
        "txn_type",
        "txn_time",
        "merchant",
        "amount_bucket",
        "cumulative_amount",
        "amount_bucket_2",
        "prev_txn_time",
        F.sum("session_start_flag").over(w6).alias("session_no_2"),
    )
)

flagged = (
    sessionized_transactions_2
    .withColumn("is_new_burst", F.when(F.lag("txn_time").over(w7).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w7) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessions_transactions = (
    flagged
    .select(
        "id",
        "txn_type",
        "txn_time",
        "amount_bucket",
        "amount_bucket_2",
        "prev_txn_time",
        F.sum("is_new_burst").over(w8).alias("session_id"),
    )
)

enriched = (
    sessions_transactions
    .select(
        "id",
        "txn_type",
        "txn_time",
        "amount_bucket",
        "amount_bucket_2",
        F.min("amount_bucket").over(w_hist).alias("last4_min_amount_bucket"),
        F.count("amount_bucket").over(w_hist).alias("last4_count_amount_bucket"),
        F.avg("amount_bucket").over(w_hist).alias("last4_avg_amount_bucket"),
        F.sum("amount_bucket_2").over(w10).alias("amount_bucket_2_sum_90d"),
    )
)

result = (
    enriched
    .groupBy(
        F.when(F.col("amount_bucket_2_sum_90d") >= 38, "HIGH").otherwise("LOW").alias("amount_bucket_2_sum_90d_tier"),
        F.when(F.col("amount_bucket_2") >= 7, "PREMIUM").otherwise("STANDARD").alias("amount_bucket_2_tier"),
        "txn_type",
    )
    .agg(
        F.size(F.collect_set("txn_type")).alias("num_txn_types"),
        F.max("last4_avg_amount_bucket").alias("max_last4_avg_amount_bucket"),
    )
    .orderBy(F.col("max_last4_avg_amount_bucket").asc_nulls_last())
)
