from pyspark.sql import functions as F

product_stats = (
    df_order_items
    .filter(F.col("quantity").isNull())
    .groupBy("product_id")
    .agg(F.avg("unit_price").alias("avg_unit_price"))
)

result = (
    df_products.alias("p")
    .join(product_stats.alias("P"), F.col("p.id") == F.col("P.product_id"), "inner")
    .select(F.col("p.id"), F.col("p.supplier_id"), F.col("P.avg_unit_price"))
    .orderBy("supplier_id")
)
