from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_customers.alias("customers"), F.col("orders.customer_id") == F.col("customers.id"), "inner")
    .join(df_stores.alias("stores"), F.col("orders.store_id") == F.col("stores.id"), "inner")
    .filter((F.col("stores.city") != "Berlin") | (F.col("stores.opened_date") >= "2025-06-01"))
    .select(
        F.col("orders.id"),
        F.col("orders.status"),
        F.col("customers.credit_limit"),
        F.col("stores.opened_date"),
        F.col("stores.region"),
        F.upper(F.col("customers.city")).alias("city_upper"),
    )
    .orderBy("status")
)
