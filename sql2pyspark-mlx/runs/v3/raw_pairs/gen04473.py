from pyspark.sql import functions as F

order_items_by_product = (
    df_order_items
    .groupBy("product_id")
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.stddev("unit_price"), 2).alias("stddev_unit_price"),
    )
)

result = (
    df_products.alias("p")
    .join(order_items_by_product.alias("t"), F.col("p.id") == F.col("t.product_id"), "inner")
    .filter(F.col("t.stddev_unit_price") >= 33)
    .select(
        F.col("p.product_name"),
        F.col("p.supplier_id"),
        F.col("t.cnt"),
        F.col("t.stddev_unit_price"),
    )
    .orderBy("cnt")
)
