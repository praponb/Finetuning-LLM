from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_stores.alias("stores"), F.col("orders.store_id") == F.col("stores.id"), "left")
    .join(df_customers.alias("customers"), F.col("orders.customer_id") == F.col("customers.id"), "left")
    .select(
        F.col("orders.id"),
        F.col("orders.store_id"),
        F.col("orders.order_date"),
        F.col("stores.store_name"),
        F.col("customers.segment"),
    )
)
