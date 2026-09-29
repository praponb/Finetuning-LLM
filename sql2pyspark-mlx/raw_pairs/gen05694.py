from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("status")
    .pivot("method", ["bank_transfer", "cash", "promptpay"])
    .agg(F.max("amount"))
    .orderBy("status")
)
