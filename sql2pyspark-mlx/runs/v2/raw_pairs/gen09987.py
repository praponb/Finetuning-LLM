from pyspark.sql import functions as F

product_summary = (
    df_store_sales
    .groupBy("product_id")
    .agg(F.round(F.avg("units"), 2).alias("avg_units"))
)

result = (
    df_products.alias("products")
    .join(product_summary.alias("product_summary"), F.col("products.id") == F.col("product_summary.product_id"), "inner")
    .select(
        F.col("products.id"),
        F.col("products.brand"),
        F.col("products.cost"),
        F.col("product_summary.avg_units"),
    )
)
