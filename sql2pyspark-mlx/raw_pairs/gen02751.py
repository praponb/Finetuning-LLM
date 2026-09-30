from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("merchant") == "Shell") & (F.col("txn_time") < "2025-02-01") & ~F.col("is_flagged"))
    .groupBy("merchant")
    .agg(
        F.min("txn_time").alias("min_txn_time"),
        F.sum(F.when(F.col("txn_type") == "CREDIT", 1).otherwise(0)).alias("credit_count"),
    )
    .orderBy("credit_count")
)
