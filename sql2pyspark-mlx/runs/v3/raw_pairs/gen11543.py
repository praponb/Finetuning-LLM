from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_customers.alias("customers"), F.col("orders.customer_id") == F.col("customers.id"), "inner")
    .join(df_stores.alias("stores"), F.col("orders.store_id") == F.col("stores.id"), "inner")
    .filter(F.col("orders.shipping_fee") <= 24)
    .select(
        F.col("orders.id"),
        F.col("orders.store_id"),
        F.col("customers.signup_date"),
        F.col("customers.segment"),
        F.col("stores.city"),
    )
)
