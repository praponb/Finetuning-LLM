from pyspark.sql import functions as F

product_summary = (
    df_order_items
    .groupBy("product_id")
    .agg(F.min("quantity").alias("min_quantity"))
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "inner")
    .filter(F.col("ps.min_quantity") > 6.7)
    .select(F.col("p.product_name"), F.col("ps.min_quantity"))
    .orderBy(F.col("min_quantity").desc())
)
