from pyspark.sql import functions as F

product_totals = (
    df_order_items
    .filter(F.col("quantity").isNull())
    .groupBy("product_id")
    .agg(F.count("unit_price").alias("unit_price_count"))
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("pt"), F.col("p.id") == F.col("pt.product_id"), "inner")
    .filter(F.col("pt.unit_price_count") > 5)
    .select(F.col("p.price"), F.col("pt.unit_price_count"))
    .orderBy("price", F.col("unit_price_count").desc())
)
