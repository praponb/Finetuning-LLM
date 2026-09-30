from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("method")
    .pivot("status", ["SUCCESS", "REFUNDED"])
    .agg(F.sum("amount"))
)
