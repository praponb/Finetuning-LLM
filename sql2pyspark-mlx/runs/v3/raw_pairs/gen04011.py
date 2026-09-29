from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isin([12, 26]) | ~F.col("method").isin(["promptpay", "paypal", "card"]))
    .withColumn("paid_at_label", F.date_format("paid_at", "yyyy-MM"))
    .orderBy(F.col("id").desc(), "method")
    .limit(3)
)
