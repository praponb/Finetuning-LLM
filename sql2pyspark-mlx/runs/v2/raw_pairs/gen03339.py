from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("status")
    .pivot("method", ["card", "bank_transfer", "paypal"])
    .agg(F.sum("amount"))
)
