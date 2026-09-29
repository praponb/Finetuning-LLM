from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_stores.alias("stores"), F.col("store_sales.store_id") == F.col("stores.id"), "inner")
    .join(df_products.alias("products"), F.col("store_sales.product_id") == F.col("products.id"), "left")
    .filter(F.upper(F.col("stores.store_name")).like("%OWN%"))
    .select(
        F.col("store_sales.id"),
        F.col("store_sales.sale_date"),
        F.col("store_sales.product_id"),
        F.col("stores.city"),
        F.col("products.cost"),
        F.col("products.product_name"),
        F.round(F.col("store_sales.revenue"), 2).alias("revenue_rounded"),
    )
    .orderBy(F.col("city").desc())
)
