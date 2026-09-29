from pyspark.sql import functions as F

product_stats = (
    df_inventory
    .filter(F.col("product_id") == 2)
    .groupBy("product_id")
    .agg(
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
        F.count("*").alias("total_count"),
    )
)

result = (
    df_products.alias("p")
    .join(product_stats.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "inner")
    .select(
        F.col("p.cost"),
        F.col("p.price"),
        F.col("ps.stddev_quantity"),
        F.col("ps.total_count"),
    )
    .orderBy("stddev_quantity")
)
