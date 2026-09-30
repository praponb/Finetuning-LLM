from pyspark.sql import functions as F

result = (
    df_stores.alias("stores")
    .join(df_orders.alias("orders"), F.col("stores.id") == F.col("orders.store_id"), "left")
    .select(
        F.col("stores.sqft"),
        F.col("stores.store_name"),
        F.col("orders.status"),
        F.col("orders.store_id"),
        F.col("orders.discount"),
    )
)
