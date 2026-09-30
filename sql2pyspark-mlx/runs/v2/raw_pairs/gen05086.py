from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_stores.alias("stores"), F.col("store_sales.store_id") == F.col("stores.id"), "inner")
    .join(df_products.alias("products"), F.col("store_sales.product_id") == F.col("products.id"), "left")
    .filter(F.col("store_sales.revenue") >= 2900)
    .groupBy(
        F.date_format(F.col("products.launch_date"), "yyyy-MM").alias("launch_month"),
    )
    .agg(F.countDistinct(F.col("store_sales.units")).alias("unique_stores"))
)
