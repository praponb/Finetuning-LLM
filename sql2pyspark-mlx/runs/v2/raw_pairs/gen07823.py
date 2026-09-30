from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("account_id", "merchant")
    .agg(
        F.avg("amount").alias("avg_amount"),
        F.max("txn_time").alias("latest_txn_time"),
    )
    .filter(F.col("avg_amount") < 1000)
    .orderBy("merchant", "account_id")
    .limit(10)
)
