from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(F.date_trunc("MONTH", "paid_at").alias("paid_month"))
    .agg(F.avg("amount").alias("avg_amount"))
    .filter(F.col("avg_amount") < 670)
    .select("paid_month", "avg_amount")
    .orderBy("avg_amount")
)
