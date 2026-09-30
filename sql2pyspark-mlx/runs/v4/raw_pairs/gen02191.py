from pyspark.sql import functions as F

product_stats = (
    df_inventory
    .filter(F.col("store_id").isin([1, 2, 4, 5]))
    .groupBy("product_id")
    .agg(F.count("*").alias("num_inventory"))
)

result = (
    df_products.alias("p")
    .join(product_stats.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "inner")
    .select(
        F.col("p.id"),
        F.col("p.launch_date"),
        F.col("p.product_name"),
        F.col("ps.num_inventory"),
    )
    .orderBy(F.col("launch_date").desc())
)
