from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("channel")
    .pivot("status", ["RETURNED", "PENDING", "SHIPPED"])
    .agg(F.sum("shipping_fee"))
    .orderBy("channel")
)
