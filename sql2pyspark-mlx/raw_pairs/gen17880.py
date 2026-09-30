from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
amount_by_account_id = df_transactions.groupBy("account_id").agg(F.max("amount").alias("max_transaction_amount"))

prev_events = df_transactions.withColumn("prev_txn_time", F.lag("txn_time").over(w))

with_gaps = (
    prev_events
    .withColumn("is_new_burst", F.when(F.col("prev_txn_time").isNull() | (F.col("txn_time") > F.col("prev_txn_time") + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessions_transactions = (
    with_gaps
    .select(
        "id",
        "account_id",
        "txn_type",
        "txn_time",
        "merchant",
        F.sum("is_new_burst").over(w2).alias("island_id"),
    )
)

result = (
    sessions_transactions
    .join(amount_by_account_id, on="account_id", how="left")
    .select(
        "id",
        "account_id",
        "txn_type",
        "merchant",
        "island_id",
        F.when(F.col("merchant").rlike("^(Shell|7-Eleven)$"), "EXTENDED").otherwise("PRIORITY").alias("merchant_zone"),
        F.col("max_transaction_amount"),
    )
    .orderBy("merchant")
)
