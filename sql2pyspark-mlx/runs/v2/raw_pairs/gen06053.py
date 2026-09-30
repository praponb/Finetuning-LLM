from pyspark.sql import functions as F

order_items_by_product = (
    df_order_items
    .groupBy("product_id")
    .agg(
        (F.max("unit_price") - F.min("unit_price")).alias("unit_price_range"),
        F.max("unit_price").alias("max_unit_price"),
    )
)

result = (
    df_products.alias("products")
    .join(order_items_by_product.alias("order_items_by_product"), F.col("products.id") == F.col("order_items_by_product.product_id"), "inner")
    .filter(F.col("order_items_by_product.unit_price_range") >= 40)
    .select(
        F.col("products.price"),
        F.col("products.category"),
        F.col("order_items_by_product.unit_price_range"),
        F.col("order_items_by_product.max_unit_price"),
    )
    .orderBy("category", F.col("price").desc())
)
