from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isNull())
    .groupBy("status", F.date_trunc("MONTH", "paid_at").alias("paid_month"))
    .agg(F.count("amount").alias("amount_count"))
)
