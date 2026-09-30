from pyspark.sql import functions as F

product_summary = (
    df_order_items
    .groupBy("product_id")
    .agg(F.sum("unit_price").alias("total_unit_price"))
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "inner")
    .filter(F.col("ps.total_unit_price") >= 2000)
    .select(F.col("p.price"), F.col("ps.total_unit_price"))
    .orderBy(F.col("total_unit_price").desc())
)
