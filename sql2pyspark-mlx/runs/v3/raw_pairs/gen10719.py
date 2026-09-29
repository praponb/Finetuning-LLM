from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method").isNull() & F.col("status").isin(["REFUNDED", "SUCCESS", "FAILED"]))
    .select("id", "paid_at", "amount", "order_id")
)
