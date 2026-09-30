from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_stores.alias("stores"), F.col("orders.store_id") == F.col("stores.id"), "inner")
    .join(df_customers.alias("customers"), F.col("orders.customer_id") == F.col("customers.id"), "inner")
    .filter((F.col("orders.order_date") < "2025-01-24") | (F.col("stores.opened_date") <= "2024-06-27"))
    .select(
        F.col("orders.customer_id"),
        F.col("orders.amount"),
        F.col("stores.region"),
        F.col("stores.opened_date"),
        F.col("customers.segment"),
        F.col("customers.city"),
    )
)
