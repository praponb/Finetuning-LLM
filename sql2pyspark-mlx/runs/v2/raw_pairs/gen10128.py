from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.hour("paid_at").between(9, 16))
    .groupBy("status")
    .pivot("method", ["cash", "paypal", "bank_transfer"])
    .agg(F.sum("amount"))
)
