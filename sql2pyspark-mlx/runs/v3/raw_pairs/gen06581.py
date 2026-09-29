from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method").isin(["promptpay", "card", "cash"]))
    .select("id", "amount", "method", "paid_at")
)
