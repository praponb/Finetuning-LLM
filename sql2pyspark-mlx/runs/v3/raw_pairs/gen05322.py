from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("method") == "cash") & (F.col("status") == "FAILED") & (F.col("order_id") == 18))
    .groupBy("status")
    .agg(F.round(F.avg("amount"), 2).alias("avg_amount"))
    .orderBy(F.col("avg_amount").desc())
)
