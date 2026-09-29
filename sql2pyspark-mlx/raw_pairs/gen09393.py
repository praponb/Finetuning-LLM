from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("channel")
    .pivot("status", ["PENDING", "COMPLETED", "RETURNED", "CANCELLED"])
    .agg(F.sum("amount"))
    .orderBy("channel")
)
