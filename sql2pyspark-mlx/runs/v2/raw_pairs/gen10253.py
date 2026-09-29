from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(F.to_date("txn_time").alias("txn_day"), "is_flagged")
    .agg(
        F.max("amount").alias("max_amount"),
        F.count(F.when(F.col("merchant") == "Amazon", 1)).alias("amazon_count"),
        F.max("txn_time").alias("latest_txn_time"),
    )
    .orderBy(F.col("max_amount").desc())
)
