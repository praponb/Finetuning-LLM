from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_type") == "FEE")
    .groupBy("is_flagged")
    .agg(F.round(F.avg("amount"), 2).alias("avg_amount"))
)
