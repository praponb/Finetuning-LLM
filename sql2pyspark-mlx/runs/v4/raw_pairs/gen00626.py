from pyspark.sql import functions as F

recent_transactions = df_transactions.filter(F.col("amount").between(1400, 3100))

agg = (
    recent_transactions
    .groupBy("is_flagged", "txn_type")
    .agg(F.round(F.avg("amount"), 2).alias("avg_amount"))
)

result = (
    agg
    .filter(F.col("avg_amount") > 62)
    .select("is_flagged", "txn_type", "avg_amount")
    .orderBy(F.col("txn_type").desc())
)
