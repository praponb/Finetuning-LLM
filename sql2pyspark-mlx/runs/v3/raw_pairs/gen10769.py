from pyspark.sql import functions as F

result = (
    df_stores.alias("stores")
    .join(df_orders.alias("orders"), F.col("stores.id") == F.col("orders.store_id"), "inner")
    .select(
        F.col("stores.region"),
        F.col("stores.city"),
        F.col("stores.store_name"),
        F.col("orders.order_date"),
    )
)
