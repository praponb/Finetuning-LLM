from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method").isin(["promptpay", "cash"]))
    .groupBy("status")
    .pivot("method", ["promptpay", "bank_transfer"])
    .agg(F.max("amount"))
    .orderBy("status")
)
