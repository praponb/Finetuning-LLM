from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_products.alias("products"), F.col("store_sales.product_id") == F.col("products.id"), "inner")
    .join(df_stores.alias("stores"), F.col("store_sales.store_id") == F.col("stores.id"), "inner")
    .filter(F.col("products.supplier_id").isin([16, 31]) & (F.col("store_sales.revenue") <= 3400))
    .select(
        F.col("store_sales.store_id"),
        F.col("store_sales.units"),
        F.col("products.supplier_id"),
        F.col("stores.sqft"),
        F.col("stores.opened_date"),
        F.initcap(F.trim(F.col("products.category"))).alias("category_title"),
    )
)
