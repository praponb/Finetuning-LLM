from pyspark.sql import functions as F

order_items_by_product = (
    df_order_items
    .filter(F.col("unit_price") >= 210)
    .groupBy("product_id")
    .agg(F.round(F.stddev("quantity"), 2).alias("stddev_quantity"))
)

result = (
    df_products.alias("p")
    .join(order_items_by_product.alias("t"), F.col("p.id") == F.col("t.product_id"), "inner")
    .filter(F.col("t.stddev_quantity") >= 48)
    .select(F.col("p.cost"), F.col("t.stddev_quantity"))
    .orderBy(F.col("cost").desc())
)
