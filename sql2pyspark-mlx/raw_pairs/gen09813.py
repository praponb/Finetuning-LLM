from pyspark.sql import functions as F

product_summary = (
    df_store_sales
    .filter(F.col("revenue") < 3300)
    .groupBy("product_id")
    .agg(F.count("*").alias("row_count"))
)

result = (
    df_products.alias("products")
    .join(product_summary.alias("product_summary"), F.col("products.id") == F.col("product_summary.product_id"), "left")
    .select(
        F.col("products.product_name"),
        F.col("products.category"),
        F.col("product_summary.row_count"),
    )
    .orderBy(F.col("row_count").desc())
)
