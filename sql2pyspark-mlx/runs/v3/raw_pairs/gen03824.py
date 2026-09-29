from pyspark.sql import functions as F

valid_transactions = df_transactions.filter(~F.col("is_flagged") & (F.col("amount") >= 1200))

transactions_stats = (
    valid_transactions
    .groupBy("txn_type")
    .agg(F.min("txn_time").alias("first_txn_time"))
)

result = transactions_stats.orderBy("first_txn_time")
