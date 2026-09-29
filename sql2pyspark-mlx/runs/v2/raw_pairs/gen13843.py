from pyspark.sql import functions as F

valid_transactions = df_transactions.filter(F.col("amount") > 3000)

transactions_stats = (
    valid_transactions
    .groupBy(F.hour("txn_time").alias("txn_hour"), "is_flagged")
    .agg((F.max("amount") - F.min("amount")).alias("amount_range"))
)

result = transactions_stats.orderBy(F.col("amount_range").desc())
