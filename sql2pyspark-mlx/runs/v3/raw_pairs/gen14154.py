from pyspark.sql import functions as F

order_items_by_product = (
    df_order_items
    .filter(F.col("quantity") >= 4)
    .groupBy("product_id")
    .agg(F.count("*").alias("cnt"))
)

result = (
    df_products.alias("p")
    .join(order_items_by_product.alias("oibp"), F.col("p.id") == F.col("oibp.product_id"), "inner")
    .select(F.col("p.category"), F.col("oibp.cnt"))
)
