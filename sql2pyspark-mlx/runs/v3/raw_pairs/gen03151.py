from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(F.date_format("paid_at", "yyyy-MM").alias("paid_month"))
    .pivot("method", ["promptpay", "cash", "card", "paypal"])
    .agg(F.sum("amount"))
    .orderBy("paid_month")
)
