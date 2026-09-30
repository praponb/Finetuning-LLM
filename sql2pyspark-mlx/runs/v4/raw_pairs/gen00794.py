from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_products.alias("products"), F.col("store_sales.product_id") == F.col("products.id"), "outer")
    .filter(F.col("store_sales.store_id").isin([1, 4, 6]))
    .groupBy(F.col("products.category"))
    .agg(
        F.min(F.col("store_sales.units")).alias("min_units"),
        F.min(F.col("store_sales.revenue")).alias("min_revenue"),
    )
    .orderBy(F.col("min_revenue").desc())
)
