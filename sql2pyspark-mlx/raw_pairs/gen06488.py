from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("status")
    .pivot("method", ["bank_transfer", "promptpay", "card", "cash"])
    .agg(F.avg("amount"))
)
