from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_stores.alias("stores"), F.col("store_sales.store_id") == F.col("stores.id"), "inner")
    .filter(F.col("store_sales.store_id") == 6)
    .select(
        F.col("store_sales.id"),
        F.col("store_sales.units"),
        F.col("stores.city"),
        F.least(F.col("store_sales.units"), F.col("store_sales.revenue")).alias("least_units_revenue"),
    )
)
