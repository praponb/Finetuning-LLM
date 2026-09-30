from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("status")
    .pivot("method", ["card", "bank_transfer", "promptpay", "cash"])
    .agg(F.sum("amount"))
    .orderBy("status")
)
