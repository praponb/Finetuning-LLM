from pyspark.sql import functions as F

product_summary = (
    df_order_items
    .filter(F.col("unit_price") <= 130)
    .groupBy("product_id")
    .agg(F.max("quantity").alias("max_quantity"))
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "left")
    .filter(F.col("ps.max_quantity") >= 3.2)
    .select(F.col("p.supplier_id"), F.col("p.product_name"), F.col("ps.max_quantity"))
    .orderBy("supplier_id")
)
