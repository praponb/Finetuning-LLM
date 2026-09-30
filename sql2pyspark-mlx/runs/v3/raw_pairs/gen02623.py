from pyspark.sql import functions as F

recent_transactions = df_transactions.filter(F.col("is_flagged") & (F.col("txn_time") < "2023-12-01"))

summary = (
    recent_transactions
    .groupBy("txn_type")
    .agg(F.avg("amount").alias("avg_amount"))
)

result = (
    summary
    .select("txn_type", "avg_amount")
    .orderBy("avg_amount", "txn_type")
)
