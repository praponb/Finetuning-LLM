from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_type") != "REFUND")
    .cube("merchant", F.quarter("txn_time").alias("txn_quarter"))
    .agg(
        F.count("*").alias("row_count"),
        F.sum(F.when(F.col("merchant") == "Amazon", F.col("amount")).otherwise(0)).alias("amazon_amount"),
        F.min("txn_time").alias("first_txn_time"),
    )
    .orderBy("amazon_amount")
)
