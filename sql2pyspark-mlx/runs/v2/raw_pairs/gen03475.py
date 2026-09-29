from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("status")
    .pivot("method", ["promptpay", "card", "paypal", "bank_transfer"])
    .agg(F.max("amount"))
)
