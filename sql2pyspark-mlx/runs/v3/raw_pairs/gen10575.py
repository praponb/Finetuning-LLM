from pyspark.sql import functions as F

result = (
    df_orders
    .select("shipping_fee")
    .subtract(df_order_items.select(F.col("unit_price").alias("shipping_fee")))
)
