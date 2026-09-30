from pyspark.sql import functions as F

result = (
    df_payments
    .select("id", "amount", "paid_at")
    .orderBy("id")
    .limit(20)
)
