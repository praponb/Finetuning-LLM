from pyspark.sql import functions as F

result = (
    df_payments
    .select("method", "status", "paid_at", F.ceil("amount").alias("amount_whole"))
    .orderBy("paid_at")
)
