from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(F.date_trunc("MONTH", "paid_at").alias("paid_month"))
    .pivot("status", ["FAILED", "SUCCESS", "REFUNDED"])
    .agg(F.avg("amount"))
)
