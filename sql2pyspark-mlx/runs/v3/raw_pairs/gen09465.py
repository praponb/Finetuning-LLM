from pyspark.sql import functions as F

result = (
    df_stores.alias("stores")
    .join(df_store_sales.alias("store_sales"), F.col("stores.id") == F.col("store_sales.store_id"), "inner")
    .groupBy(F.col("stores.id"), F.col("stores.city"))
    .agg(
        F.count(F.col("store_sales.id")).alias("store_sale_count"),
        F.sum(F.col("store_sales.units")).alias("total_units"),
        F.min(F.col("store_sales.units")).alias("min_units"),
    )
)
