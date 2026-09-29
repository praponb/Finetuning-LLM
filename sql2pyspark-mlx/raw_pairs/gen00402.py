from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("is_flagged", "merchant")
    .agg(F.round(F.avg("amount"), 2).alias("avg_amount"))
    .orderBy("merchant", F.col("avg_amount").desc(), "is_flagged")
    .limit(50)
)
