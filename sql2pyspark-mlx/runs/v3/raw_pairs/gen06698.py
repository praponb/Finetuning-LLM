from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_stores.alias("stores"), F.col("store_sales.store_id") == F.col("stores.id"), "inner")
    .join(df_products.alias("products"), F.col("products.id") == F.col("store_sales.product_id"), "inner")
    .filter(~(F.col("store_sales.product_id").isin([2, 6, 9]) & (F.col("products.stock_qty") > 353)))
    .select(
        F.col("store_sales.revenue"),
        F.col("stores.opened_date"),
        F.col("stores.region"),
        F.col("products.product_name"),
        F.col("products.brand"),
    )
    .orderBy("region")
)
