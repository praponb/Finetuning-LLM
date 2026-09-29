from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_customers.alias("customers"), F.col("orders.customer_id") == F.col("customers.id"), "inner")
    .join(df_stores.alias("stores"), F.col("orders.store_id") == F.col("stores.id"), "right")
    .select(
        F.col("orders.customer_id"),
        F.col("customers.age"),
        F.col("customers.country"),
        F.col("stores.opened_date"),
        F.col("stores.store_name"),
    )
)
