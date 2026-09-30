from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isin([12, 18, 22, 30]) & (F.col("status") != "REFUNDED"))
    .select("id", "status", "paid_at", "amount")
    .orderBy("amount", F.col("id").desc())
)
