from pyspark.sql import functions as F

result = (
    df_payments
    .filter(((F.col("order_id") == 34) & (F.col("amount") >= 560)) | (F.col("paid_at") < "2025-02-15"))
    .select("id", "amount", "status")
)
