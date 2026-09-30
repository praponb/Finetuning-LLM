from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(F.month("txn_time").alias("txn_month"), "merchant")
    .agg(F.avg("amount").alias("avg_amount"))
    .filter(F.col("avg_amount") < 2400)
    .orderBy(F.col("txn_month").desc())
)
