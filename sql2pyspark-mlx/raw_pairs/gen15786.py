from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.filter(F.col("shipping_fee") > 29).alias("o"), F.col("oi.order_id") == F.col("o.id"), "left_anti")
    .filter(F.col("oi.quantity") > 3)
    .select(F.col("oi.id"), F.col("oi.product_id"), F.col("oi.unit_price"))
    .orderBy("id", "product_id")
)
