from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("method", "order_id")
    .pivot("status", ["SUCCESS", "FAILED"])
    .agg(F.max("amount"))
)
