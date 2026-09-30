from pyspark.sql import functions as F

result = (
    df_stores.alias("stores")
    .join(df_store_sales.alias("store_sales"), F.col("stores.id") == F.col("store_sales.store_id"), "left_semi")
    .select(
        F.col("stores.id"),
        F.col("stores.store_name"),
        F.col("stores.region"),
        F.col("stores.opened_date"),
    )
    .orderBy(F.col("region").desc())
)
