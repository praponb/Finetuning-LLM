from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(((F.col("txn_type") == "REFUND") & F.col("is_flagged")) | F.col("account_id").isin([1, 16]))
    .groupBy(
        F.when(F.col("amount") >= 3600, "high").when(F.col("amount") >= 1000, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.max("txn_time").alias("latest_txn_time"),
        F.avg("amount").alias("avg_amount"),
    )
    .filter(F.col("avg_amount") >= 2100)
    .orderBy(F.col("amount_bucket").desc())
    .limit(5)
)
