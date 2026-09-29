from pyspark.sql import functions as F

base = df_transactions.filter(F.col("account_id").isin([10, 27]))

transactions_stats = (
    base
    .groupBy("merchant")
    .agg(F.round(F.stddev("amount"), 2).alias("stddev_amount"))
)

result = (
    transactions_stats
    .orderBy("merchant", F.col("stddev_amount").desc())
    .limit(50)
)
