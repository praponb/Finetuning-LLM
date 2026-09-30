from pyspark.sql import functions as F

recent_transactions = df_transactions.filter((F.col("merchant") == "7-Eleven") & ~F.col("is_flagged"))

summary = (
    recent_transactions
    .groupBy(F.month("txn_time").alias("txn_month"))
    .agg(F.max("txn_time").alias("latest_txn_time"))
)

result = summary.select("txn_month", "latest_txn_time")
