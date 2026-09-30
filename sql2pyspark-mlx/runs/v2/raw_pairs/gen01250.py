from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_customers.alias("customers"), F.col("customers.id") == F.col("orders.customer_id"), "inner")
    .join(df_stores.alias("stores"), F.col("orders.store_id") == F.col("stores.id"), "left")
    .filter(F.col("customers.email").like("%@startup.io") & F.col("customers.name").like("%Chen%"))
    .select(
        F.col("orders.id"),
        F.col("orders.shipping_fee"),
        F.col("customers.age"),
        F.col("stores.sqft"),
        F.col("stores.store_name"),
    )
)
