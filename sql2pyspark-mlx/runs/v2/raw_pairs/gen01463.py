from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_products.alias("products"), F.col("store_sales.product_id") == F.col("products.id"), "inner")
    .join(df_stores.alias("stores"), F.col("store_sales.store_id") == F.col("stores.id"), "left")
    .groupBy(F.col("products.category"))
    .agg(
        F.round(F.stddev(F.col("store_sales.revenue")), 2).alias("stddev_revenue"),
        F.countDistinct(F.col("store_sales.revenue")).alias("unique_stores"),
    )
    .filter(F.col("stddev_revenue") >= 21)
)
