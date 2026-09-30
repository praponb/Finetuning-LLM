from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_stores.alias("stores"), F.col("store_sales.store_id") == F.col("stores.id"), "left")
    .select(
        F.col("store_sales.revenue"),
        F.col("stores.store_name"),
        F.col("stores.opened_date"),
        F.when(F.col("stores.region") == "Central", 1).otherwise(0).alias("is_central"),
    )
    .orderBy("opened_date")
)
