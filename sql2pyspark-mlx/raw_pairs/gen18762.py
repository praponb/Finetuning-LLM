from pyspark.sql import functions as F

product_summary = (
    df_store_sales
    .groupBy("product_id")
    .agg(F.count("units").alias("units_count"))
)

result = (
    df_products.alias("products")
    .join(product_summary.alias("product_summary"), F.col("products.id") == F.col("product_summary.product_id"), "inner")
    .select(
        F.col("products.id"),
        F.col("products.product_name"),
        F.col("product_summary.units_count"),
    )
)
