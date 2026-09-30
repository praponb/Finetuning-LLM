from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_type") == "FEE")
    .groupBy("is_flagged")
    .agg(F.min("txn_time").alias("min_txn_time"))
    .orderBy("min_txn_time")
)
