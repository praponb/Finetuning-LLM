from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_products.alias("products"), F.col("store_sales.product_id") == F.col("products.id"), "left")
    .join(df_stores.alias("stores"), F.col("stores.id") == F.col("store_sales.store_id"), "inner")
    .filter(F.col("products.product_name").isNotNull() & (F.col("store_sales.units") >= 12))
    .select(
        F.col("store_sales.id"),
        F.col("store_sales.units"),
        F.col("store_sales.sale_date"),
        F.col("products.brand"),
        F.col("stores.sqft"),
        F.col("stores.city"),
    )
    .orderBy(F.col("brand").desc(), F.col("sqft").desc())
)
