from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method").isin(["cash", "paypal", "bank_transfer"]) | (F.col("amount") >= 770))
    .select("id", "paid_at", "status")
)
