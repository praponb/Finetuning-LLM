from pyspark.sql import functions as F

product_totals = (
    df_store_sales
    .groupBy("product_id")
    .agg(F.sum("revenue").alias("total_revenue"))
)

result = (
    df_products.alias("products")
    .join(product_totals.alias("product_totals"), F.col("products.id") == F.col("product_totals.product_id"), "inner")
    .filter(F.col("product_totals.total_revenue") > 22000)
    .select(
        F.col("products.id"),
        F.col("products.product_name"),
        F.col("products.price"),
        F.col("product_totals.total_revenue"),
    )
    .orderBy("product_name", F.col("price").desc())
)
