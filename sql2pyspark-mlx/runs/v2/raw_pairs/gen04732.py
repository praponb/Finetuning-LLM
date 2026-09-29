from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("status")
    .pivot("method", ["cash", "promptpay", "card"])
    .agg(F.sum("amount"))
    .orderBy("status")
)
