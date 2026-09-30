from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("channel")
    .pivot("status", ["PENDING", "SHIPPED"])
    .agg(F.avg("amount"))
)
