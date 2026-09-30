from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_stores.alias("stores"), F.col("stores.id") == F.col("store_sales.store_id"), "right")
    .select(
        F.col("store_sales.store_id"),
        F.col("stores.opened_date"),
        F.col("stores.city"),
        F.col("stores.region"),
    )
    .orderBy("opened_date")
)
