from pyspark.sql import functions as F

recent_transactions = df_transactions.filter(F.col("txn_type") == "DEBIT")

transactions_stats = (
    recent_transactions
    .groupBy("is_flagged")
    .agg(
        F.avg("amount").alias("avg_amount"),
        F.max("amount").alias("max_amount"),
    )
)

result = (
    transactions_stats
    .filter(F.col("max_amount") >= 2100)
    .select("is_flagged", "max_amount")
    .orderBy(F.col("max_amount").desc())
)
