from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_order_items.alias("order_items"), F.col("order_items.product_id") == F.col("products.id"), "inner")
    .filter(F.col("order_items.unit_price") >= 100)
    .select(
        F.col("products.category"),
        F.col("order_items.unit_price"),
        F.col("order_items.product_id"),
        F.col("order_items.order_id"),
    )
    .orderBy(F.col("product_id").desc(), "category")
)
