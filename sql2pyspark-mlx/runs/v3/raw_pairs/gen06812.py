from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("txn_type", "merchant")
    .agg(F.max("txn_time").alias("latest_txn_time"))
    .orderBy("latest_txn_time", "merchant")
)
