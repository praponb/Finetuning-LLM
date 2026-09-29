from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("status")
    .pivot("method", ["paypal", "card", "cash"])
    .agg(F.avg("amount"))
)
