from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_stores.alias("stores"), F.col("store_sales.store_id") == F.col("stores.id"), "inner")
    .filter((F.col("stores.region") == "South") & (F.col("store_sales.revenue") < 1500))
    .select(
        F.col("store_sales.product_id"),
        F.col("store_sales.store_id"),
        F.col("stores.store_name"),
        F.col("stores.sqft"),
        F.col("stores.opened_date"),
        F.concat(F.col("stores.region"), F.lit(" - "), F.col("stores.city")).alias("region_city"),
    )
    .orderBy(F.col("region_city").desc(), "sqft")
)
