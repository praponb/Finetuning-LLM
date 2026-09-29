from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_customers.alias("customers"), F.col("orders.customer_id") == F.col("customers.id"), "right")
    .join(df_stores.alias("stores"), F.col("orders.store_id") == F.col("stores.id"), "inner")
    .filter((F.col("stores.sqft") > 4925) & (F.col("customers.signup_date") > "2025-06-06"))
    .select(
        F.col("orders.channel"),
        F.col("customers.country"),
        F.col("customers.email"),
        F.col("stores.opened_date"),
        F.col("stores.store_name"),
    )
)
