from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(F.quarter("txn_time").alias("txn_quarter"), "account_id")
    .agg(F.max("txn_time").alias("last_txn_time"))
)
