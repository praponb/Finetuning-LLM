from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_order_items.alias("order_items"), F.col("products.id") == F.col("order_items.product_id"), "left")
    .filter(F.col("products.supplier_id") == 11)
    .groupBy(F.col("products.id"), F.col("products.category"))
    .agg(
        F.count(F.col("order_items.id")).alias("order_item_count"),
        F.min(F.col("order_items.quantity")).alias("min_quantity"),
    )
    .orderBy(F.col("id").desc(), F.col("min_quantity").desc())
)
