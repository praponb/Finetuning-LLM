from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("method", "status", F.date_trunc("MONTH", "paid_at").alias("paid_month"))
    .agg(F.avg("amount").alias("avg_amount"))
    .orderBy(F.col("method").desc())
)
