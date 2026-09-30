from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount") >= 520)
    .select("id", "paid_at", "status")
)
