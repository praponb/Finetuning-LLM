from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_stores.alias("stores"), F.col("store_sales.store_id") == F.col("stores.id"), "right")
    .join(df_products.alias("products"), F.col("store_sales.product_id") == F.col("products.id"), "inner")
    .filter(F.col("store_sales.product_id") == 6)
    .select(
        F.col("store_sales.product_id"),
        F.col("stores.store_name"),
        F.col("products.price"),
        F.col("products.supplier_id"),
        F.last_day(F.col("store_sales.sale_date")).alias("sale_date_month_end"),
    )
)
