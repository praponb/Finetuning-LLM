from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("merchant", "txn_type")
    .agg(F.min("txn_time").alias("min_txn_time"))
    .orderBy("min_txn_time")
)
