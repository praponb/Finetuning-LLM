from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_stores.alias("stores"), F.col("store_sales.store_id") == F.col("stores.id"), "left")
    .join(df_products.alias("products"), F.col("products.id") == F.col("store_sales.product_id"), "inner")
    .filter(F.col("store_sales.units").between(14, 25))
    .groupBy(F.year(F.col("products.launch_date")).alias("launch_year"))
    .agg(F.round(F.avg(F.col("store_sales.units")), 2).alias("avg_units"))
)
