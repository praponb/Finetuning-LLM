from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([4, 11, 26, 35]))
    .groupBy("txn_type")
    .agg(F.avg("amount").alias("avg_amount"))
    .orderBy(F.col("avg_amount").desc())
)
