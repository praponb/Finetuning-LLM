from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method").isNotNull())
    .select("id", "amount", "status", "paid_at")
)
