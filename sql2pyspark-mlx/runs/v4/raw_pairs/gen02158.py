from pyspark.sql import functions as F

product_summary = (
    df_order_items
    .filter(F.col("unit_price").isNotNull())
    .groupBy("product_id")
    .agg(F.sum((F.col("quantity") * F.col("unit_price"))).alias("revenue"))
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("t"), F.col("p.id") == F.col("t.product_id"), "inner")
    .filter(F.col("t.revenue") >= 76)
    .select(F.col("p.id"), F.col("p.category"), F.col("t.revenue"))
)
