from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(
        F.when(F.col("amount") >= 3100, "high").when(F.col("amount") >= 1600, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(F.max("txn_time").alias("last_txn_time"))
    .orderBy("last_txn_time")
)
