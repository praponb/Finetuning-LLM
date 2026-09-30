from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("merchant") != "Lazada")
    .agg(
        F.min("txn_time").alias("min_txn_time"),
        F.max("txn_time").alias("last_txn_time"),
        F.count(F.when(F.col("txn_type") == "REFUND", 1)).alias("refund_count"),
    )
    .orderBy("min_txn_time")
)
