from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("order_id")
    .pivot("method", ["bank_transfer", "card"])
    .agg(F.max("amount"))
)
