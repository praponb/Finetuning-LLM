from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("status", F.date_trunc("MONTH", "paid_at").alias("paid_month"))
    .agg(F.sum("amount").alias("total_amount"))
    .filter(F.col("total_amount") < 3000)
    .select("status", "paid_month", "total_amount")
    .orderBy(F.col("paid_month").desc())
)
