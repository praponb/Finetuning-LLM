from pyspark.sql import functions as F

product_stats = (
    df_store_sales
    .filter(F.col("revenue") > 1500)
    .groupBy("product_id")
    .agg(F.count("*").alias("num_store_sales"))
)

result = (
    df_products.alias("p")
    .join(product_stats.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "inner")
    .filter(F.col("ps.num_store_sales") > 8)
    .select(
        F.col("p.id"),
        F.col("p.product_name"),
        F.col("p.price"),
        F.col("ps.num_store_sales"),
    )
    .orderBy(F.col("num_store_sales").desc())
)
