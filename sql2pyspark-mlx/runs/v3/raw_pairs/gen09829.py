from pyspark.sql import functions as F

result = (
    df_payments
    .filter(((F.col("method") != "promptpay") & (F.col("amount") >= 880)) | (F.col("method") == "bank_transfer"))
    .select("id", "paid_at")
    .orderBy("paid_at", F.col("id").desc())
    .limit(20)
)
