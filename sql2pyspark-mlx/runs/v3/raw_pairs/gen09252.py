from pyspark.sql import functions as F

product_summary = (
    df_inventory
    .filter(F.col("quantity") == 192)
    .groupBy("product_id")
    .agg(
        F.max("last_restock").alias("latest_last_restock"),
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
    )
)

result = (
    df_products.alias("products")
    .join(product_summary.alias("product_summary"), F.col("products.id") == F.col("product_summary.product_id"), "inner")
    .select(
        F.col("products.id"),
        F.col("products.cost"),
        F.col("products.product_name"),
        F.col("product_summary.latest_last_restock"),
        F.col("product_summary.stddev_quantity"),
    )
    .orderBy("product_name", F.col("id").desc())
    .limit(20)
)
