from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("channel")
    .pivot("status", ["PENDING", "RETURNED", "COMPLETED", "SHIPPED"])
    .agg(F.avg("shipping_fee"))
    .orderBy("channel")
)
