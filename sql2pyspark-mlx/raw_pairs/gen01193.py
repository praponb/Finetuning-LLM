from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_products.alias("products"), F.col("store_sales.product_id") == F.col("products.id"), "inner")
    .join(df_stores.alias("stores"), F.col("store_sales.store_id") == F.col("stores.id"), "left")
    .select(
        F.col("store_sales.id"),
        F.col("store_sales.product_id"),
        F.col("store_sales.revenue"),
        F.col("products.price"),
        F.col("stores.city"),
        (F.datediff(F.current_date(), F.col("store_sales.sale_date")) / 365.25).alias("years_since_sale_date"),
    )
)
