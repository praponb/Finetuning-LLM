from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_orders.alias("orders"), F.col("orders.id") == F.col("order_items.order_id"), "left")
    .join(df_products.alias("products"), F.col("order_items.product_id") == F.col("products.id"), "left")
    .filter(F.col("order_items.quantity").between(5, 8))
    .groupBy(F.col("orders.status"), F.col("orders.channel"))
    .agg(F.countDistinct(F.col("order_items.quantity")).alias("unique_orders"))
    .orderBy(F.col("status").desc())
)
