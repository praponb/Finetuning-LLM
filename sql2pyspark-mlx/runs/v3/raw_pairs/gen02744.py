from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount") > 720)
    .select("id", "status", "paid_at", "order_id")
    .orderBy("id")
)
