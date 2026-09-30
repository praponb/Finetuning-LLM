from pyspark.sql import functions as F

valid_transactions = df_transactions.filter(F.col("is_flagged"))

transactions_stats = (
    valid_transactions
    .groupBy("is_flagged")
    .agg(F.count("*").alias("total_count"))
)

result = (
    transactions_stats
    .filter(F.col("total_count") >= 5)
    .select("is_flagged", "total_count")
    .orderBy(F.col("total_count").desc(), "is_flagged")
    .limit(10)
)
