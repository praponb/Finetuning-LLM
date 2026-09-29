from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isNotNull() & ((F.col("amount") >= 640) | (F.col("method") == "promptpay")))
    .groupBy(F.month("paid_at").alias("paid_month"))
    .agg(F.count("amount").alias("amount_count"))
    .filter(F.col("amount_count") > 3)
    .orderBy("paid_month")
)
