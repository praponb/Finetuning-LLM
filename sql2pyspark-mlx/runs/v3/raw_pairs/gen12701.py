from pyspark.sql import functions as F

product_stats = (
    df_order_items
    .filter(F.col("unit_price") > 190)
    .groupBy("product_id")
    .agg(F.min("quantity").alias("min_quantity"))
)

result = (
    df_products.alias("p")
    .join(product_stats.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "inner")
    .select(
        F.col("p.id"),
        F.col("p.launch_date"),
        F.col("p.brand"),
        F.col("ps.min_quantity"),
    )
)
