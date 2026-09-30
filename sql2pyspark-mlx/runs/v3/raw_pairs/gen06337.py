from pyspark.sql import functions as F

product_summary = (
    df_order_items
    .filter(F.col("quantity") != 4)
    .groupBy("product_id")
    .agg(F.count("*").alias("row_count"))
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("P"), F.col("p.id") == F.col("P.product_id"), "inner")
    .select(F.col("p.cost"), F.col("P.row_count"))
)
