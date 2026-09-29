from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_products.alias("products"), F.col("products.id") == F.col("store_sales.product_id"), "inner")
    .join(df_stores.alias("stores"), F.col("store_sales.store_id") == F.col("stores.id"), "inner")
    .filter(F.year(F.col("stores.opened_date")) == 2025)
    .groupBy(F.col("stores.region"))
    .agg(F.round(F.avg(F.col("store_sales.revenue")), 2).alias("avg_revenue"))
)
