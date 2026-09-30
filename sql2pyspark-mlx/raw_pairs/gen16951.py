from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount").isNull())
    .groupBy("method")
    .pivot("status", ["REFUNDED", "FAILED"])
    .agg(F.avg("amount"))
)
