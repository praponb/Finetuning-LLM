from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("paid_at") >= "2023-05-01")
    .select("id", "amount", "paid_at")
)
