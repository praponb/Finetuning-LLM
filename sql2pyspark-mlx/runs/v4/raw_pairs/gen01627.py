from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy(F.col("txn_time_dayofweek").desc_nulls_first(), "id")
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id")
w3 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w5 = Window.partitionBy("merchant").orderBy("txn_time_date", "id")
w6 = Window.orderBy("new_group_flag", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("account_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("merchant").orderBy("txn_time", "id")
w9 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_transactions
    .filter(F.col("txn_time") >= F.to_date(F.lit("2023-06-24")))
    .withColumn("txn_time_date", F.to_date("txn_time"))
    .withColumn("txn_time_dayofweek", F.dayofweek("txn_time"))
)

ranked = (
    filtered
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

with_gaps = (
    ranked
    .withColumn("new_group_flag", F.when(F.lag("txn_time").over(w2).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w2) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("session_no", F.sum("new_group_flag").over(w3))

metrics = (
    sessionized
    .select(
        "*",
        F.sum("session_no").over(w3).alias("cumulative_session_no"),
        F.max("txn_time_dayofweek").over(w4).alias("txn_time_dayofweek_max_90d"),
        F.row_number().over(w5).alias("event_seq"),
        F.last("merchant", ignorenulls=True).over(w6).alias("last_known_merchant"),
        F.first("amount", ignorenulls=True).over(w7).alias("first_known_amount"),
    )
)

with_prev = metrics.withColumn("prev_txn_time", F.lag("txn_time").over(w8))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_txn_time").isNull() | (F.col("txn_time") > F.col("prev_txn_time") + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessions_transactions = flagged.withColumn("island_id", F.sum("is_new_session").over(w9))

totals = (
    sessions_transactions
    .groupBy(F.date_trunc("YEAR", "txn_time_date").alias("year_start"))
    .agg(
        F.max(F.when(F.col("txn_type") == "REFUND", F.col("txn_time_dayofweek_max_90d"))).alias("refund_max_txn_time_dayofweek_max_90d"),
        F.max("prev_txn_time").alias("last_seen"),
        F.sum(F.when(F.col("txn_type") == "DEBIT", F.col("new_group_flag")).otherwise(0)).alias("debit_new_group_flag"),
        F.avg("first_known_amount").alias("avg_first_known_amount"),
        F.size(F.collect_set("txn_type")).alias("num_txn_types"),
        F.avg(F.when(F.col("merchant") == "Amazon", F.col("is_new_session"))).alias("amazon_avg_is_new_session"),
    )
)

result = (
    totals
    .select(
        "year_start",
        "refund_max_txn_time_dayofweek_max_90d",
        "last_seen",
        "debit_new_group_flag",
        "avg_first_known_amount",
    )
    .orderBy(F.col("debit_new_group_flag").asc_nulls_last())
)
