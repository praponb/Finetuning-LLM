from pyspark.sql import functions as F

result = (
    df_stores.alias("stores")
    .join(df_store_sales.alias("store_sales"), F.col("store_sales.store_id") == F.col("stores.id"), "left")
    .filter(F.col("stores.sqft") > 7310)
    .groupBy(F.col("stores.id"), F.col("stores.city"))
    .agg(
        F.count(F.col("store_sales.id")).alias("store_sale_count"),
        F.min(F.col("store_sales.units")).alias("min_units"),
    )
    .filter(F.col("store_sale_count") > 1)
)
