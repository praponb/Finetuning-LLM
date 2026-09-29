from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(
        F.when(F.col("amount") >= 3900, "high").when(F.col("amount") >= 1600, "mid").otherwise("low").alias("amount_bucket"),
        F.month("txn_time").alias("txn_month"),
    )
    .agg(
        F.min("txn_time").alias("min_txn_time"),
        F.round(((100 * F.sum(F.when(F.col("txn_type") == "REFUND", 1).otherwise(0))) / F.count("*")), 1).alias("pct_refund"),
    )
    .filter(F.col("pct_refund") >= 52)
    .orderBy("amount_bucket", F.col("txn_month").desc())
)
