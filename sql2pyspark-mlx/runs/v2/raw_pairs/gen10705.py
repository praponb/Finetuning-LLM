from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("status")
    .pivot("method", ["paypal", "promptpay", "card"])
    .agg(F.sum("amount"))
)
