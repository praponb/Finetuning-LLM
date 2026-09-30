from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("merchant", "account_id")
    .agg(
        F.avg("amount").alias("avg_amount"),
        F.max("txn_time").alias("last_txn_time"),
    )
    .filter(F.col("avg_amount") >= 2300)
    .orderBy("last_txn_time", "merchant", "account_id")
    .limit(10)
)
