from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_stores.alias("stores"), F.col("orders.store_id") == F.col("stores.id"), "inner")
    .join(df_customers.alias("customers"), F.col("customers.id") == F.col("orders.customer_id"), "left")
    .select(
        F.col("orders.id"),
        F.col("orders.customer_id"),
        F.col("orders.status"),
        F.col("stores.sqft"),
        F.col("customers.city"),
        F.col("customers.is_active"),
    )
)
