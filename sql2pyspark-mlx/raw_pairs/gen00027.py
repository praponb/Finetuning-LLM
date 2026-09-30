from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("status")
    .pivot("method", ["paypal", "bank_transfer", "cash"])
    .agg(F.sum("amount"))
)
