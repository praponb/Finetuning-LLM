from pyspark.sql import functions as F

product_summary = (
    df_order_items
    .filter(F.col("quantity") <= 6)
    .groupBy("product_id")
    .agg(
        F.countDistinct("unit_price").alias("unique_products"),
        F.count("unit_price").alias("unit_price_count"),
    )
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "left")
    .filter(F.col("ps.unique_products") >= 7)
    .select(
        F.col("p.id"),
        F.col("p.launch_date"),
        F.col("ps.unique_products"),
        F.col("ps.unit_price_count"),
    )
)
