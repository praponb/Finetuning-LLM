from pyspark.sql import functions as F

recent_payments = (
    df_payments
    .filter(F.col("method") == "promptpay")
    .withColumn("days_since_paid_at", F.datediff(F.current_date(), "paid_at"))
)

payments_stats = (
    recent_payments
    .groupBy("order_id")
    .agg(F.min("paid_at").alias("min_paid_at"))
)

result = payments_stats.select("order_id", "min_paid_at")
