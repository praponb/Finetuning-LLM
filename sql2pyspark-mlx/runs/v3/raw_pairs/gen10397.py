from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("amount") <= 2600) & F.col("is_flagged") & F.col("is_flagged"))
    .groupBy("is_flagged", F.month("txn_time").alias("txn_month"))
    .agg(
        F.sum(F.when(F.col("merchant") == "Amazon", 1).otherwise(0)).alias("amazon_count"),
        F.countDistinct("amount").alias("unique_txn_types"),
        F.count("*").alias("total_count"),
    )
)
