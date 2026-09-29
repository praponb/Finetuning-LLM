from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_stores.alias("stores"), F.col("stores.id") == F.col("store_sales.store_id"), "left")
    .select(
        F.col("store_sales.id"),
        F.col("store_sales.units"),
        F.col("store_sales.product_id"),
        F.col("stores.store_name"),
        F.col("stores.sqft"),
    )
    .orderBy(F.col("units").desc(), F.col("store_name").desc(), "id")
    .limit(10)
)
