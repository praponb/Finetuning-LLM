from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([16, 17, 26, 30]))
    .groupBy("txn_type", F.to_date("txn_time").alias("txn_day"))
    .agg(
        F.sum(F.when(F.col("merchant") == "Amazon", F.col("amount")).otherwise(0)).alias("amazon_amount"),
        F.max("txn_time").alias("last_txn_time"),
    )
    .orderBy("last_txn_time")
)
