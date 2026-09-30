from pyspark.sql import functions as F

result = (
    df_payments
    .filter(~F.col("method").isin(["bank_transfer", "promptpay"]) & (F.to_date("paid_at") < "2025-05-01"))
    .select("method", "paid_at", "status")
    .orderBy("method")
)
