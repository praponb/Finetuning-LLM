from pyspark.sql import functions as F

result = (
    df_payments
    .filter(~F.col("method").isin(["cash", "promptpay", "bank_transfer"]) & (F.col("order_id").isin([23, 24, 34, 38]) | (F.col("paid_at") >= "2024-01-01")))
    .select("id", "amount", "method")
)
