from pyspark.sql import functions as F

recent_transactions = (
    df_transactions
    .filter(F.col("account_id").isin([5, 8, 29]) & F.col("account_id").isin([15, 16]))
)

summary = (
    recent_transactions
    .groupBy("merchant")
    .agg(F.count("*").alias("transaction_count"))
)

result = summary
