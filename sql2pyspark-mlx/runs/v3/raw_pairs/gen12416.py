from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_products.alias("products"), F.col("products.id") == F.col("order_items.product_id"), "left")
    .join(df_orders.alias("orders"), F.col("order_items.order_id") == F.col("orders.id"), "inner")
    .filter(F.col("orders.customer_id") == 18)
    .groupBy(F.col("products.category"))
    .agg(F.round(F.avg(F.col("order_items.quantity")), 2).alias("avg_quantity"))
    .filter(F.col("avg_quantity") > 75)
    .orderBy(F.col("category").desc())
)
