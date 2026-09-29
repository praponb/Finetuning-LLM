from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("paid_at") >= "2024-01-01") & ((F.col("amount") <= 850) | (F.col("amount") < 850)))
    .select("method", "paid_at", "order_id")
    .orderBy("order_id")
)
