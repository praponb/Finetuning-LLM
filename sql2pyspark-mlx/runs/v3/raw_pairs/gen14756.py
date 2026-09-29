from pyspark.sql import functions as F

product_stats = (
    df_inventory
    .groupBy("product_id")
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        F.min("last_restock").alias("first_last_restock"),
    )
)

result = (
    df_products.alias("p")
    .join(product_stats.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "left")
    .select(
        F.col("p.id"),
        F.col("p.launch_date"),
        F.col("p.price"),
        F.col("ps.avg_quantity"),
        F.col("ps.first_last_restock"),
    )
)
