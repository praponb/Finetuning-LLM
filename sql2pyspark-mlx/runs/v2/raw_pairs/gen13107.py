from pyspark.sql import functions as F

product_summary = (
    df_order_items
    .groupBy("product_id")
    .agg(F.sum((F.col("quantity") * F.col("unit_price"))).alias("revenue"))
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("P"), F.col("p.id") == F.col("P.product_id"), "left")
    .filter(F.col("P.revenue") > 39)
    .select(F.col("p.launch_date"), F.col("P.revenue"))
    .orderBy("launch_date", F.col("revenue").desc())
)
