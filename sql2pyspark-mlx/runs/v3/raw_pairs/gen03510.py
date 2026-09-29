from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("merchant") == "Shell") & F.hour("txn_time").between(12, 19))
    .agg(F.avg("amount").alias("avg_amount"))
    .orderBy(F.col("avg_amount").desc())
)
