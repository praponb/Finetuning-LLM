from pyspark.sql import functions as F

order_items_by_product = (
    df_order_items
    .groupBy("product_id")
    .agg(
        F.max("unit_price").alias("max_unit_price"),
        F.count("*").alias("cnt"),
    )
)

result = (
    df_products.alias("products")
    .join(order_items_by_product.alias("t"), F.col("products.id") == F.col("t.product_id"), "inner")
    .select(
        F.col("products.id"),
        F.col("products.cost"),
        F.col("t.max_unit_price"),
        F.col("t.cnt"),
    )
)
