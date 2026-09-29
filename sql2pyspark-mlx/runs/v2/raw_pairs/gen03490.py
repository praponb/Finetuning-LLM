from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method").isin(["promptpay", "paypal", "card"]))
    .groupBy("order_id")
    .agg(F.min("paid_at").alias("min_paid_at"))
)
