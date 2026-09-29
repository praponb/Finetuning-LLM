from pyspark.sql import functions as F

base = df_transactions.filter(F.col("txn_type") == "REFUND")

agg = (
    base
    .groupBy("txn_type")
    .agg(F.round(F.avg("amount"), 2).alias("avg_amount"))
)

result = (
    agg
    .filter(F.col("avg_amount") > 23)
    .select("txn_type", "avg_amount")
)
