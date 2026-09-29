from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_stores.alias("stores"), F.col("store_sales.store_id") == F.col("stores.id"), "left")
    .join(df_products.alias("products"), F.col("store_sales.product_id") == F.col("products.id"), "left")
    .filter((F.col("stores.sqft") < 11249) & F.lower(F.col("stores.store_name")).like("%irp%"))
    .select(
        F.col("store_sales.units"),
        F.col("store_sales.sale_date"),
        F.col("stores.region"),
        F.col("stores.city"),
        F.col("products.brand"),
        F.col("products.category"),
    )
)
