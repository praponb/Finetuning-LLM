from pyspark.sql import functions as F

order_items_by_product = (
    df_order_items
    .groupBy("product_id")
    .agg(
        F.max("unit_price").alias("max_unit_price"),
        F.count("unit_price").alias("unit_price_count"),
    )
)

result = (
    df_products.alias("p")
    .join(order_items_by_product.alias("oibp"), F.col("p.id") == F.col("oibp.product_id"), "inner")
    .filter(F.col("oibp.max_unit_price") >= 150)
    .select(
        F.col("p.cost"),
        F.col("p.launch_date"),
        F.col("oibp.max_unit_price"),
        F.col("oibp.unit_price_count"),
    )
    .orderBy(F.col("cost").desc())
)
