from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([5, 8, 16, 29]))
    .groupBy("is_flagged", "merchant")
    .agg(F.avg("amount").alias("avg_amount"))
    .filter(F.col("avg_amount") >= 3600)
    .orderBy(F.col("avg_amount").desc())
)
