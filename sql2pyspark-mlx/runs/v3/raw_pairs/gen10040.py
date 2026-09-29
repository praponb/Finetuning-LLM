from pyspark.sql import functions as F

product_stats = (
    df_inventory
    .groupBy("product_id")
    .agg(
        F.count("*").alias("row_count"),
        F.min("quantity").alias("min_quantity"),
    )
)

result = (
    df_products.alias("p")
    .join(product_stats.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "inner")
    .filter(F.col("ps.row_count") >= 4)
    .select(F.col("p.cost"), F.col("ps.row_count"), F.col("ps.min_quantity"))
    .orderBy("row_count")
)
