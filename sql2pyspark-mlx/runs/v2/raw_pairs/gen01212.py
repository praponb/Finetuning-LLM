from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("merchant").isin(["Amazon", "Shell"]) & F.hour("txn_time").between(6, 12))
    .groupBy("account_id")
    .agg(F.avg("amount").alias("avg_amount"))
    .orderBy(F.col("account_id").desc())
)
