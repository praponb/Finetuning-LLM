from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method").isin(["cash", "bank_transfer", "promptpay"]) & (F.col("amount") > 1170) & (F.to_date("paid_at") < "2024-10-01"))
    .select("id", "method", "paid_at", "amount")
    .orderBy("paid_at", "id")
    .limit(10)
)
