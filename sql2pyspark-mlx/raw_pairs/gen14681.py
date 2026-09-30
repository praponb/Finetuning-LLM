from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("txn_type") != "CREDIT") & F.col("is_flagged"))
    .groupBy("merchant")
    .agg(F.avg("amount").alias("avg_amount"))
    .filter(F.col("avg_amount") >= 1400)
    .orderBy(F.col("avg_amount").desc())
)
