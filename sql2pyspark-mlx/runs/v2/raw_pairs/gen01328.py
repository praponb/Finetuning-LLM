from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_stores.alias("stores"), F.col("orders.store_id") == F.col("stores.id"), "left")
    .filter(F.col("stores.store_name").like("%Tow%"))
    .select(
        F.col("orders.id"),
        F.col("orders.order_date"),
        F.col("orders.amount"),
        F.col("orders.store_id"),
        F.col("stores.city"),
        F.col("stores.opened_date"),
    )
)
