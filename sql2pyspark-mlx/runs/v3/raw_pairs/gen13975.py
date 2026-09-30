from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("customer_id")
    .pivot("status", ["SHIPPED", "COMPLETED"])
    .agg(F.count("shipping_fee"))
)
