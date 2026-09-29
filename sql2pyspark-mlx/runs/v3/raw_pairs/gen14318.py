from pyspark.sql import functions as F

order_items_by_product = (
    df_order_items
    .filter(F.col("unit_price") <= 160)
    .groupBy("product_id")
    .agg(F.sum((F.col("unit_price") * F.col("quantity"))).alias("revenue"))
)

result = (
    df_products.alias("p")
    .join(order_items_by_product.alias("oibp"), F.col("p.id") == F.col("oibp.product_id"), "left")
    .select(F.col("p.id"), F.col("p.price"), F.col("oibp.revenue"))
    .orderBy("id")
)
