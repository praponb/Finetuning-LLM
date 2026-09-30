from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("channel")
    .pivot("status", ["SHIPPED", "PENDING", "CANCELLED", "COMPLETED"])
    .agg(F.count("shipping_fee"))
)
