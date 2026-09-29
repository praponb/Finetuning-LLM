from pyspark.sql import functions as F

product_totals = (
    df_order_items
    .groupBy("product_id")
    .agg(F.round(F.avg("quantity"), 2).alias("avg_quantity"))
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("P"), F.col("p.id") == F.col("P.product_id"), "inner")
    .select(F.col("p.category"), F.col("p.supplier_id"), F.col("P.avg_quantity"))
)
