from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("status")
    .pivot("method", ["promptpay", "cash", "paypal"])
    .agg(F.sum("amount"))
)
