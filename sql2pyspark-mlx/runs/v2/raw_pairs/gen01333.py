from pyspark.sql import functions as F

product_stats = (
    df_inventory
    .filter(F.col("product_id") == 9)
    .groupBy("product_id")
    .agg(
        F.countDistinct("quantity").alias("unique_stores"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
    )
)

result = (
    df_products.alias("p")
    .join(product_stats.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "inner")
    .select(
        F.col("p.id"),
        F.col("p.price"),
        F.col("ps.unique_stores"),
        F.col("ps.avg_quantity"),
    )
    .orderBy(F.col("price").desc())
)
