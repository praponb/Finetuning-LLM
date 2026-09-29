from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("merchant").isin(["Shell", "Lazada", "Starbucks"]) | F.col("amount").isNotNull())
    .groupBy(
        F.when(F.col("amount") >= 3100, "high").when(F.col("amount") >= 1900, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(F.sum("amount").alias("total_amount"))
)
