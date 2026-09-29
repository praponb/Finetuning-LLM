from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("status")
    .pivot("method", ["bank_transfer", "card", "promptpay", "paypal"])
    .agg(F.count("amount"))
)
