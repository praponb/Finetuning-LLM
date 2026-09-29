from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("merchant", "txn_type", "is_flagged")
    .agg(F.max("txn_time").alias("last_txn_time"))
)
