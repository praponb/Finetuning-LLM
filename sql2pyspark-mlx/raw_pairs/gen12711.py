from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
amount_by_account_id = df_transactions.groupBy("account_id").agg(F.sum("amount").alias("sum_transaction_amount"))

prev_events = df_transactions.withColumn("prev_txn_time", F.lag("txn_time").over(w))

with_gaps = (
    prev_events
    .withColumn("new_group_flag", F.when(F.col("prev_txn_time").isNull() | (F.col("txn_time") > F.col("prev_txn_time") + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

islands = (
    with_gaps
    .select(
        "id",
        "account_id",
        "txn_type",
        "txn_time",
        "merchant",
        "is_flagged",
        F.sum("new_group_flag").over(w2).alias("session_id"),
    )
)

result = (
    islands
    .join(amount_by_account_id, on="account_id", how="left")
    .select(
        "id",
        "account_id",
        "txn_type",
        "txn_time",
        "merchant",
        "is_flagged",
        F.when(F.col("merchant").rlike("^(Lazada|Starbucks)$"), "GROUP_A").when(F.col("merchant").rlike("^(Grab|7-Eleven)$"), "PRIORITY").otherwise("GROUP_B").alias("merchant_zone"),
        F.col("sum_transaction_amount"),
    )
)
