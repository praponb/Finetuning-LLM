from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount") < 760)
    .select("id", "paid_at", "status")
)
