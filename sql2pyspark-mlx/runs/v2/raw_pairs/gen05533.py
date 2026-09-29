from pyspark.sql import functions as F

valid_payments = (
    df_payments
    .filter(F.col("method").isin(["promptpay", "card"]) & F.col("status").isin(["REFUNDED", "SUCCESS"]) & (F.col("amount") >= 1460))
)

agg = (
    valid_payments
    .groupBy(F.to_date("paid_at").alias("paid_day"))
    .agg(F.avg("amount").alias("avg_amount"))
)

result = (
    agg
    .filter(F.col("avg_amount") >= 1030)
    .select("paid_day", "avg_amount")
    .orderBy(F.col("avg_amount").desc())
)
