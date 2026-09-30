from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method") != "bank_transfer")
    .select("id", "amount", "paid_at")
)
