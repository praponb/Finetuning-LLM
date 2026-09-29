from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_orders.alias("orders"), F.col("order_items.order_id") == F.col("orders.id"), "right")
    .join(df_products.alias("products"), F.col("order_items.product_id") == F.col("products.id"), "inner")
    .filter(F.col("products.product_name").isNotNull())
    .groupBy(F.year(F.col("products.launch_date")).alias("launch_year"))
    .agg(F.count("*").alias("order_item_count"))
)
