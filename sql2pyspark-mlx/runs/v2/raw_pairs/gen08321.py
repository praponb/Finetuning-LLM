from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(F.year("txn_time").alias("txn_year"), "merchant")
    .agg(F.round(F.avg("amount"), 2).alias("avg_amount"))
    .filter(F.col("avg_amount") >= 71)
    .orderBy(F.col("merchant").desc(), "txn_year")
    .limit(3)
)
