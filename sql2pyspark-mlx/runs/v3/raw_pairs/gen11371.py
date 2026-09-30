from pyspark.sql import functions as F

product_summary = (
    df_order_items
    .filter(F.col("unit_price") < 290)
    .groupBy("product_id")
    .agg(F.min("quantity").alias("min_quantity"))
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "left")
    .select(F.col("p.brand"), F.col("p.product_name"), F.col("ps.min_quantity"))
    .orderBy("product_name", "min_quantity")
)
