from pyspark.sql import functions as F

order_items_by_product = (
    df_order_items
    .groupBy("product_id")
    .agg(
        F.avg("unit_price").alias("avg_unit_price"),
        F.min("unit_price").alias("min_unit_price"),
    )
)

result = (
    df_products.alias("p")
    .join(order_items_by_product.alias("oibp"), F.col("p.id") == F.col("oibp.product_id"), "inner")
    .filter(F.col("oibp.avg_unit_price") >= 350)
    .select(
        F.col("p.id"),
        F.col("p.category"),
        F.col("oibp.avg_unit_price"),
        F.col("oibp.min_unit_price"),
    )
)
