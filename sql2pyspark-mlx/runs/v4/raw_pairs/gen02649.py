from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_customers.alias("customers"), F.col("orders.customer_id") == F.col("customers.id"), "inner")
    .join(df_stores.alias("stores"), F.col("orders.store_id") == F.col("stores.id"), "left")
    .filter(F.lower(F.col("customers.email")).like("%iam%"))
    .select(
        F.col("orders.customer_id"),
        F.col("orders.shipping_fee"),
        F.col("customers.credit_limit"),
        F.col("customers.age"),
        F.col("stores.store_name"),
        F.col("stores.opened_date"),
    )
    .orderBy("shipping_fee")
)
