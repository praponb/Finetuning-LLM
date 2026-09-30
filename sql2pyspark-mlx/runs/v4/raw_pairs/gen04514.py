from pyspark.sql import functions as F

order_items_by_product = (
    df_order_items
    .filter(F.col("product_id") == 27)
    .groupBy("product_id")
    .agg(F.count("unit_price").alias("unit_price_count"))
)

result = (
    df_products.alias("p")
    .join(order_items_by_product.alias("oibp"), F.col("p.id") == F.col("oibp.product_id"), "inner")
    .select(F.col("p.category"), F.col("oibp.unit_price_count"))
    .orderBy(F.col("category").desc(), "unit_price_count")
)
