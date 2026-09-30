from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_stores.alias("stores"), F.col("stores.id") == F.col("store_sales.store_id"), "left")
    .join(df_products.alias("products"), F.col("store_sales.product_id") == F.col("products.id"), "left")
    .filter(~F.col("stores.city").isin(["Tokyo", "Berlin"]) & (F.col("stores.opened_date") >= "2023-07-01"))
    .select(
        F.col("store_sales.store_id"),
        F.col("stores.region"),
        F.col("products.product_name"),
    )
)
