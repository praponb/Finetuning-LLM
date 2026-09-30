from pyspark.sql import functions as F

product_stats = (
    df_inventory
    .groupBy("product_id")
    .agg(F.round(F.stddev("quantity"), 2).alias("stddev_quantity"))
)

result = (
    df_products.alias("products")
    .join(product_stats.alias("t"), F.col("products.id") == F.col("t.product_id"), "left")
    .filter(F.col("t.stddev_quantity") >= 32)
    .select(
        F.col("products.id"),
        F.col("products.launch_date"),
        F.col("products.product_name"),
        F.col("t.stddev_quantity"),
    )
    .orderBy("product_name")
)
