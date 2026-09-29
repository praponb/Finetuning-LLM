from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount").between(3600, 3800))
    .groupBy("is_flagged", "txn_type", "merchant")
    .agg(
        F.count("amount").alias("amount_count"),
        F.max("txn_time").alias("last_txn_time"),
    )
)
