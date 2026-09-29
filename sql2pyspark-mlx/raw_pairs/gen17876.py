from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount").isNotNull())
    .select("id", "status", "order_id", "paid_at")
)
