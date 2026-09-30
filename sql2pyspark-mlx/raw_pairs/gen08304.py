from pyspark.sql import functions as F

product_stats = (
    df_inventory
    .groupBy("product_id")
    .agg(
        F.count("*").alias("num_inventory"),
        F.max("last_restock").alias("latest_last_restock"),
    )
)

result = (
    df_products.alias("products")
    .join(product_stats.alias("product_stats"), F.col("products.id") == F.col("product_stats.product_id"), "inner")
    .select(
        F.col("products.id"),
        F.col("products.price"),
        F.col("product_stats.num_inventory"),
        F.col("product_stats.latest_last_restock"),
    )
    .orderBy(F.col("num_inventory").desc())
)
