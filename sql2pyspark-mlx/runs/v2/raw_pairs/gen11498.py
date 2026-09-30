from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("amount") >= 480) & (F.col("amount") <= 550))
    .groupBy("status")
    .agg(F.round(F.avg("amount"), 2).alias("avg_amount"))
    .orderBy("status", "avg_amount")
)
