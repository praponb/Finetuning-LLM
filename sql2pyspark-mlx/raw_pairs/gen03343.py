from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_products.alias("products"), F.col("products.id") == F.col("store_sales.product_id"), "inner")
    .join(df_stores.alias("stores"), F.col("stores.id") == F.col("store_sales.store_id"), "inner")
    .filter(F.col("products.stock_qty") == 336)
    .groupBy(F.col("products.brand"), F.col("products.category"))
    .agg(F.sum(F.col("store_sales.units")).alias("total_units"))
    .orderBy("category", "brand")
)
