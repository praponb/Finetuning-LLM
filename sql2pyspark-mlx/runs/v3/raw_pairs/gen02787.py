from pyspark.sql import functions as F

product_stats = (
    df_inventory
    .groupBy("product_id")
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.count("*").alias("num_inventory"),
    )
)

result = (
    df_products.alias("products")
    .join(product_stats.alias("t"), F.col("products.id") == F.col("t.product_id"), "left")
    .filter(F.col("t.num_inventory") >= 6)
    .select(
        F.col("products.id"),
        F.col("products.category"),
        F.col("products.launch_date"),
        F.col("t.total_quantity"),
        F.col("t.num_inventory"),
    )
    .orderBy(F.col("num_inventory").desc())
)
