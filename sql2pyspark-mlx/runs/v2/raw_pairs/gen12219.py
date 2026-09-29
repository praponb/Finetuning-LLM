from pyspark.sql import functions as F

valid_transactions = df_transactions.filter(~F.col("is_flagged"))

transactions_stats = (
    valid_transactions
    .groupBy("merchant", "txn_type")
    .agg(F.min("txn_time").alias("min_txn_time"))
)

result = transactions_stats.orderBy(F.col("min_txn_time").desc())
