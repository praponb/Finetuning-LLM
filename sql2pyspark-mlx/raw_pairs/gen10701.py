from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("status")
    .pivot("method", ["bank_transfer", "card", "paypal", "cash"])
    .agg(F.avg("amount"))
    .orderBy("status")
)
