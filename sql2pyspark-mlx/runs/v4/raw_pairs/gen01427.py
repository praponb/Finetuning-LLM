from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(
        F.when(F.col("amount") >= 3300, "high").when(F.col("amount") >= 1700, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("merchant") == "Lazada", F.col("amount")).otherwise(0)).alias("lazada_amount"),
        F.min("txn_time").alias("first_txn_time"),
        F.max("txn_time").alias("last_txn_time"),
    )
    .orderBy("first_txn_time")
)
