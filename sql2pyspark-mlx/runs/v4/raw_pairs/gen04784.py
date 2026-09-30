from pyspark.sql import functions as F

result = (
    df_order_items
    .select(F.col("unit_price").alias("value"))
    .intersect(df_orders.select(F.col("shipping_fee").alias("value")))
    .orderBy("value")
)
