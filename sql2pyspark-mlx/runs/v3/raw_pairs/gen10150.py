from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("status")
    .pivot("method", ["promptpay", "bank_transfer", "card"])
    .agg(F.sum("amount"))
    .orderBy("status")
)
