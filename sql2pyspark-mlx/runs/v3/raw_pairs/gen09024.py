from pyspark.sql import functions as F

src = (
    df_transactions
    .filter(F.col("account_id").isin([13, 25, 29, 40]))
    .withColumn("merchant_prefix", F.substring("merchant", 1, 3))
)

transactions_stats = (
    src
    .groupBy("is_flagged", "txn_type")
    .agg(
        F.min("txn_time").alias("min_txn_time"),
        F.count("*").alias("num_transactions"),
    )
)

result = (
    transactions_stats
    .select("is_flagged", "min_txn_time", "num_transactions")
    .orderBy(F.col("num_transactions").desc(), "is_flagged")
)
