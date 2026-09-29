from pyspark.sql import functions as F

product_stats = (
    df_store_sales
    .filter(F.col("revenue") >= 2500)
    .groupBy("product_id")
    .agg(
        F.min("revenue").alias("min_revenue"),
        F.round(F.stddev("revenue"), 2).alias("stddev_revenue"),
    )
)

result = (
    df_products.alias("p")
    .join(product_stats.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "inner")
    .filter(F.col("ps.stddev_revenue") >= 63)
    .select(
        F.col("p.id"),
        F.col("p.price"),
        F.col("ps.min_revenue"),
        F.col("ps.stddev_revenue"),
    )
    .orderBy("id", F.col("price").desc())
)
