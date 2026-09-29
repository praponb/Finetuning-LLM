from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.filter(F.col("discount") <= 0.19).alias("o"), F.col("oi.order_id") == F.col("o.id"), "left_anti")
    .filter(F.col("oi.unit_price").between(200, 380))
    .select(F.col("oi.id"), F.col("oi.product_id"), F.col("oi.quantity"))
)
