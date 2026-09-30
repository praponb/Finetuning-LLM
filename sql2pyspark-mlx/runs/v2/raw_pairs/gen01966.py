from pyspark.sql import functions as F

valid_transactions = df_transactions.filter(F.col("merchant") != "Grab")

transactions_stats = (
    valid_transactions
    .groupBy("is_flagged", F.date_format("txn_time", "yyyy-MM").alias("txn_month"))
    .agg(F.count("*").alias("cnt"))
)

result = transactions_stats
