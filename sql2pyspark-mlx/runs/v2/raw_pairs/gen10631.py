from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.count("*").alias("total_count"),
        F.min("txn_time").alias("min_txn_time"),
    )
)
