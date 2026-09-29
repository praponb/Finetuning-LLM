from pyspark.sql import functions as F

product_summary = (
    df_order_items
    .groupBy("product_id")
    .agg(F.count("unit_price").alias("unit_price_count"))
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "left")
    .filter(F.col("ps.unit_price_count") > 5)
    .select(F.col("p.id"), F.col("p.supplier_id"), F.col("ps.unit_price_count"))
    .orderBy(F.col("id").desc())
)
