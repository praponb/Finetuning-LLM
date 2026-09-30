from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.hour("paid_at").between(7, 13))
    .groupBy("status")
    .pivot("method", ["card", "bank_transfer", "promptpay", "paypal"])
    .agg(F.max("amount"))
)
