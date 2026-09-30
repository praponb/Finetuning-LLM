from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_stores.alias("stores"), F.col("orders.store_id") == F.col("stores.id"), "outer")
    .join(df_customers.alias("customers"), F.col("orders.customer_id") == F.col("customers.id"), "inner")
    .select(
        F.col("orders.id"),
        F.col("orders.channel"),
        F.col("orders.order_date"),
        F.col("stores.opened_date"),
        F.col("stores.city"),
        F.col("customers.signup_date"),
        F.col("customers.age"),
    )
)
