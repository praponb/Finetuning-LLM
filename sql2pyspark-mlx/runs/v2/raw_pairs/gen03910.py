from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_order_items.alias("order_items"), F.col("products.id") == F.col("order_items.product_id"), "left")
    .filter(F.col("order_items.unit_price").between(160, 200))
    .groupBy(F.col("products.id"), F.col("products.category"))
    .agg(
        F.count(F.col("order_items.id")).alias("order_item_count"),
        F.max(F.col("order_items.unit_price")).alias("max_unit_price"),
    )
    .orderBy("category", "order_item_count")
)
