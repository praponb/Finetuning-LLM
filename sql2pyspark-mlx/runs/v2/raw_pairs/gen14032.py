from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_customers.alias("customers"), F.col("orders.customer_id") == F.col("customers.id"), "left")
    .join(df_stores.alias("stores"), F.col("orders.store_id") == F.col("stores.id"), "inner")
    .select(
        F.col("orders.id"),
        F.col("orders.store_id"),
        F.col("orders.status"),
        F.col("customers.signup_date"),
        F.col("customers.is_active"),
        F.col("stores.opened_date"),
        F.col("stores.region"),
    )
)
