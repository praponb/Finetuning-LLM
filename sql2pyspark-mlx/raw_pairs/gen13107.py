from pyspark.sql import functions as F

product_stats = (
    df_order_items
    .groupBy("product_id")
    .agg(
        F.round(F.stddev("unit_price"), 2).alias("stddev_unit_price"),
        F.sum("unit_price").alias("total_unit_price"),
    )
)

result = (
    df_products.alias("p")
    .join(product_stats.alias("P"), F.col("p.id") == F.col("P.product_id"), "inner")
    .filter(F.col("P.stddev_unit_price") > 34)
    .select(
        F.col("p.id"),
        F.col("p.supplier_id"),
        F.col("P.stddev_unit_price"),
        F.col("P.total_unit_price"),
    )
    .orderBy(F.col("stddev_unit_price").desc(), "id")
)
