from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("customer_id")
    .pivot("channel", ["web", "mobile", "store"])
    .agg(F.sum("shipping_fee"))
    .orderBy("customer_id")
)
