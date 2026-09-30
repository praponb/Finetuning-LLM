from pyspark.sql import functions as F

result = (
    df_stores.alias("stores")
    .join(df_orders.alias("orders"), F.col("stores.id") == F.col("orders.store_id"), "left")
    .select(
        F.col("stores.region"),
        F.col("stores.opened_date"),
        F.col("orders.order_date"),
        F.col("orders.discount"),
        F.col("orders.shipping_fee"),
    )
)
