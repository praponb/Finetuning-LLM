from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("is_flagged", "txn_type")
    .agg(F.avg("amount").alias("avg_amount"))
    .filter(F.col("avg_amount") > 3200)
    .orderBy("txn_type", F.col("avg_amount").desc(), "is_flagged")
    .limit(5)
)
