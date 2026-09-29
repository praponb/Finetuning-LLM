from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_order_items.alias("order_items"), F.col("order_items.product_id") == F.col("products.id"), "left")
    .groupBy(F.col("products.id"), F.col("products.brand"))
    .agg(F.count(F.col("order_items.id")).alias("order_item_count"))
    .filter(F.col("order_item_count") > 1)
)
