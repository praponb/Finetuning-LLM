from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method").isin(["cash", "paypal"]) | (F.col("status") == "FAILED"))
    .select("id", "paid_at")
)
