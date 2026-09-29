from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_stores.alias("stores"), F.col("orders.store_id") == F.col("stores.id"), "inner")
    .filter(F.col("orders.discount").between(0.14, 0.38) & (F.col("stores.opened_date") < "2025-07-06"))
    .select(
        F.col("orders.customer_id"),
        F.col("orders.order_date"),
        F.col("orders.store_id"),
        F.col("stores.region"),
        F.col("stores.store_name"),
    )
)
