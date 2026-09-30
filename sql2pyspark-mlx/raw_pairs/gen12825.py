from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("amount") <= 2700) & ((F.col("amount") <= 3500) | (F.col("txn_time") >= "2024-09-06")))
    .agg(
        F.count("*").alias("total_count"),
        F.min("amount").alias("min_amount"),
    )
)
