from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("amount") <= 750) & (F.col("method") == "paypal"))
    .groupBy("order_id", F.date_trunc("MONTH", "paid_at").alias("paid_month"))
    .agg(
        F.max("paid_at").alias("latest_paid_at"),
        F.count(F.when(F.col("status") == "REFUNDED", 1)).alias("refunded_count"),
    )
    .orderBy(F.col("refunded_count").desc())
)
