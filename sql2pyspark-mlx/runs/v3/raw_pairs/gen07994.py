from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("method")
    .pivot("status", ["REFUNDED", "FAILED"])
    .agg(F.max("amount"))
    .orderBy("method")
)
