from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_type") == "DEBIT")
    .groupBy("is_flagged", F.date_format("txn_time", "yyyy-MM").alias("txn_month"))
    .agg(
        F.max("txn_time").alias("last_txn_time"),
        F.max("txn_time").alias("latest_txn_time"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
    )
    .orderBy(F.col("txn_month").desc(), "is_flagged")
    .limit(1)
)
