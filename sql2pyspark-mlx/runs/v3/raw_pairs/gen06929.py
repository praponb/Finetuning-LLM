from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isin([2, 14]))
    .select("amount", "method", "paid_at", F.lower("method").alias("method_lower"))
    .orderBy("paid_at", "method_lower")
)
