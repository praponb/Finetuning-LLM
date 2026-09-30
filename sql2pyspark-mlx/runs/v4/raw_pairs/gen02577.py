from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(
        "method",
        F.when(F.col("amount") >= 1420, "high").when(F.col("amount") >= 790, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("method") == "bank_transfer", 1).otherwise(0)).alias("bank_transfer_count"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
    )
    .filter(F.col("bank_transfer_count") < 59)
)
