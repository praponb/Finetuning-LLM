from pyspark.sql import functions as F

recent_transactions = df_transactions.filter(F.col("merchant") == "Grab")

summary = (
    recent_transactions
    .groupBy("account_id")
    .agg(
        F.avg("amount").alias("avg_amount"),
        F.count(F.when(F.col("txn_type") == "REFUND", 1)).alias("refund_count"),
    )
)

result = summary.select("account_id", "avg_amount", "refund_count")
