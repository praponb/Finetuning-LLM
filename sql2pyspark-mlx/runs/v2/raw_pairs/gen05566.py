from pyspark.sql import functions as F

result = (
    df_stores.alias("stores")
    .join(df_store_sales.alias("store_sales"), F.col("stores.id") == F.col("store_sales.store_id"), "inner")
    .select(
        F.col("stores.store_name"),
        F.col("store_sales.store_id"),
        F.col("store_sales.units"),
        F.col("store_sales.product_id"),
    )
    .orderBy("units")
)
