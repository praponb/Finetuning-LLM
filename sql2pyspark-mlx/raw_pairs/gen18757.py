from pyspark.sql import functions as F

recent_transactions = (
    df_transactions
    .filter(F.col("amount") > 2800)
    .withColumn("amount_scaled", F.round((F.col("amount") * 0.9), 2))
)

transactions_stats = (
    recent_transactions
    .groupBy("is_flagged", F.quarter("txn_time").alias("txn_quarter"))
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.sum(F.when(F.col("txn_type") == "FEE", 1).otherwise(0)).alias("fee_count"),
    )
)

result = transactions_stats.select("is_flagged", "txn_quarter", "avg_amount", "fee_count")
