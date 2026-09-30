from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(F.quarter("paid_at").alias("paid_quarter"))
    .pivot("method", ["bank_transfer", "promptpay"])
    .agg(F.sum("amount"))
    .orderBy("paid_quarter")
)
