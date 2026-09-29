from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("account_id", F.date_trunc("MONTH", "txn_time").alias("txn_month"))
    .agg(F.max("txn_time").alias("last_txn_time"))
    .orderBy(F.col("account_id").desc(), "txn_month")
    .limit(3)
)
