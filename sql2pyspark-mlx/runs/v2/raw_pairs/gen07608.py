from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("status")
    .pivot("method", ["cash", "paypal", "bank_transfer"])
    .agg(F.sum("amount"))
    .orderBy("status")
)
