from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("txn_type", "merchant")
    .agg(
        F.sum(F.when(F.col("merchant") == "Amazon", 1).otherwise(0)).alias("amazon_count"),
        F.avg("amount").alias("avg_amount"),
    )
    .filter(F.col("avg_amount") >= 2500)
    .drop("avg_amount")
)
