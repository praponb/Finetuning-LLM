from pyspark.sql import functions as F

result = (
    df_stores.alias("stores")
    .join(df_store_sales.alias("store_sales"), F.col("store_sales.store_id") == F.col("stores.id"), "left")
    .select(
        F.col("stores.region"),
        F.col("stores.opened_date"),
        F.col("stores.city"),
        F.col("store_sales.sale_date"),
        F.col("store_sales.revenue"),
        F.col("store_sales.store_id"),
        F.substring(F.col("stores.store_name"), 1, 3).alias("store_name_prefix"),
    )
    .orderBy("store_name_prefix")
)
