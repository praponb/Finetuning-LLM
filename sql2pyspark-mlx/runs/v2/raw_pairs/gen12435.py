from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("order_id") == 2) | F.col("status").isin(["REFUNDED", "FAILED", "SUCCESS"]))
    .select("id", "paid_at", "order_id")
    .orderBy("paid_at")
)
