from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method").isin(["promptpay", "bank_transfer"]) & F.col("order_id").isin([22, 30]))
    .groupBy("order_id", "method")
    .agg(F.max("paid_at").alias("latest_paid_at"))
)
