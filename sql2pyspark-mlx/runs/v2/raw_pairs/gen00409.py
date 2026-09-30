from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_time") >= "2023-06-01")
    .groupBy("is_flagged")
    .agg(
        F.max("txn_time").alias("latest_txn_time"),
        F.min("amount").alias("min_amount"),
    )
    .filter(F.col("min_amount") < 4000)
    .select("is_flagged", "latest_txn_time", "min_amount")
    .orderBy("latest_txn_time", F.col("min_amount").desc(), "is_flagged")
    .limit(1)
)
