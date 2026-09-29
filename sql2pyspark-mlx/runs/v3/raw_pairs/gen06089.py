from pyspark.sql import functions as F

product_totals = (
    df_store_sales
    .groupBy("product_id")
    .agg(
        F.round(F.avg("units"), 2).alias("avg_units"),
        F.round(F.avg("revenue"), 2).alias("avg_revenue"),
    )
)

result = (
    df_products.alias("products")
    .join(product_totals.alias("product_totals"), F.col("products.id") == F.col("product_totals.product_id"), "inner")
    .select(
        F.col("products.id"),
        F.col("products.category"),
        F.col("product_totals.avg_units"),
        F.col("product_totals.avg_revenue"),
    )
)
