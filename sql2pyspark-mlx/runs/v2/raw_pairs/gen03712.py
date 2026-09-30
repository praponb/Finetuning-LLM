from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_stores.alias("stores"), F.col("store_sales.store_id") == F.col("stores.id"), "inner")
    .filter((F.col("stores.opened_date") < "2025-03-10") & (F.col("store_sales.revenue") >= 2600))
    .select(
        F.col("store_sales.id"),
        F.col("store_sales.store_id"),
        F.col("stores.store_name"),
        F.col("stores.sqft"),
        F.concat(F.col("stores.region"), F.lit(", "), F.col("stores.store_name")).alias("region_store_name"),
    )
)
