from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("amount") <= 1140) & F.col("status").isin(["FAILED", "SUCCESS", "REFUNDED"]))
    .select("order_id", "method", "paid_at")
    .orderBy(F.col("order_id").desc())
)
