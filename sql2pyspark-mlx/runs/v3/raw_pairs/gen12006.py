from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("txn_type")
    .agg(F.avg("amount").alias("avg_amount"))
    .filter(F.col("avg_amount") < 2300)
    .select("txn_type", "avg_amount")
)
