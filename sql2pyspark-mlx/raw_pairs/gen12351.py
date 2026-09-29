from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_products.alias("products"), F.col("products.id") == F.col("store_sales.product_id"), "inner")
    .join(df_stores.alias("stores"), F.col("stores.id") == F.col("store_sales.store_id"), "outer")
    .groupBy(F.col("stores.region"), F.col("stores.city"))
    .agg(F.avg(F.col("store_sales.revenue")).alias("avg_revenue"))
    .orderBy("city", "region")
    .limit(50)
)
