from pyspark.sql import functions as F

product_summary = (
    df_store_sales
    .groupBy("product_id")
    .agg(F.sum("units").alias("total_units"))
)

result = (
    df_products.alias("products")
    .join(product_summary.alias("product_summary"), F.col("products.id") == F.col("product_summary.product_id"), "inner")
    .filter(F.col("product_summary.total_units") >= 240)
    .select(
        F.col("products.price"),
        F.col("products.category"),
        F.col("product_summary.total_units"),
    )
    .orderBy("price")
)
