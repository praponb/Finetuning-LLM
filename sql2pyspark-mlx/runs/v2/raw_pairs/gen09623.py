from pyspark.sql import functions as F

product_stats = (
    df_inventory
    .groupBy("product_id")
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        F.count("*").alias("cnt"),
    )
)

result = (
    df_products.alias("p")
    .join(product_stats.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "inner")
    .filter(F.col("ps.cnt") >= 5)
    .select(F.col("p.id"), F.col("p.cost"), F.col("ps.avg_quantity"), F.col("ps.cnt"))
)
