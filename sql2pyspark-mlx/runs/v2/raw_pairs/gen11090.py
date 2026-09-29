from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isNotNull())
    .select("id", "amount", "order_id", "paid_at")
)
