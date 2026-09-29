from pyspark.sql import functions as F

result = (
    df_stores.alias("stores")
    .join(df_store_sales.alias("store_sales"), F.col("stores.id") == F.col("store_sales.store_id"), "left")
    .filter(F.col("store_sales.product_id").isin([5, 9]))
    .select(
        F.col("stores.city"),
        F.col("stores.region"),
        F.col("stores.sqft"),
        F.col("store_sales.store_id"),
        F.col("store_sales.units"),
        F.col("store_sales.product_id"),
    )
    .orderBy(F.col("store_id").desc())
)
