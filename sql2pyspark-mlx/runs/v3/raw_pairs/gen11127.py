from pyspark.sql import functions as F

product_stats = (
    df_order_items
    .filter(F.col("order_id").isin([29, 34]))
    .groupBy("product_id")
    .agg(F.round(F.stddev("quantity"), 2).alias("stddev_quantity"))
)

result = (
    df_products.alias("p")
    .join(product_stats.alias("P"), F.col("p.id") == F.col("P.product_id"), "inner")
    .filter(F.col("P.stddev_quantity") > 70)
    .select(F.col("p.supplier_id"), F.col("P.stddev_quantity"))
)
