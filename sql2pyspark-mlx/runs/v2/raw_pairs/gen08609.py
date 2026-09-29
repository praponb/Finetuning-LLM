from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_stores.alias("stores"), F.col("stores.id") == F.col("store_sales.store_id"), "left")
    .groupBy(F.col("stores.city"))
    .agg(F.max(F.col("store_sales.revenue")).alias("max_revenue"))
)
