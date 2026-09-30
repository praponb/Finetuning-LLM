from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isNotNull() & (F.col("method") != "promptpay"))
    .select("id", "method", F.date_format("paid_at", "yyyy-MM").alias("paid_at_label"))
)
