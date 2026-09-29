from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_products.alias("products"), F.col("store_sales.product_id") == F.col("products.id"), "inner")
    .join(df_stores.alias("stores"), F.col("store_sales.store_id") == F.col("stores.id"), "left")
    .filter(F.col("products.category") == "Grocery")
    .select(
        F.col("store_sales.store_id"),
        F.col("products.cost"),
        F.col("stores.region"),
        F.col("stores.sqft"),
    )
    .orderBy("store_id")
)
