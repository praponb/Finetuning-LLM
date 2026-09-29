from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount") < 3500)
    .groupBy(F.month("txn_time").alias("txn_month"))
    .agg(
        F.min("txn_time").alias("min_txn_time"),
        F.count("*").alias("transaction_count"),
    )
    .orderBy(F.col("transaction_count").desc(), F.col("txn_month").desc())
    .limit(5)
)
