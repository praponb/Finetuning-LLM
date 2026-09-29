from pyspark.sql import functions as F

base = df_transactions.filter((F.col("txn_time") < "2025-08-01") & F.col("is_flagged"))

transactions_stats = (
    base
    .groupBy("merchant")
    .agg(F.round(F.stddev("amount"), 2).alias("stddev_amount"))
)

result = transactions_stats.orderBy(F.col("stddev_amount").desc(), F.col("merchant").desc())
