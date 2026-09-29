from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("txn_type", "is_flagged")
    .agg(
        F.min("txn_time").alias("min_txn_time"),
        F.round(((100 * F.sum(F.when(F.col("merchant") == "Lazada", 1).otherwise(0))) / F.count("*")), 1).alias("pct_lazada"),
        F.avg("amount").alias("avg_amount"),
        F.count("amount").alias("amount_count"),
    )
    .filter(F.col("amount_count") >= 3)
    .drop("amount_count")
)
