from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([8, 37]) & (F.col("amount") >= 1600))
    .groupBy("is_flagged", "account_id")
    .agg(
        F.min("txn_time").alias("first_txn_time"),
        F.min("txn_time").alias("min_txn_time"),
    )
)
