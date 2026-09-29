from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_stores.alias("stores"), F.col("orders.store_id") == F.col("stores.id"), "inner")
    .join(df_customers.alias("customers"), F.col("orders.customer_id") == F.col("customers.id"), "inner")
    .filter(F.col("orders.discount").between(0.12, 0.23) & F.col("stores.region").isNotNull())
    .select(
        F.col("orders.customer_id"),
        F.col("orders.store_id"),
        F.col("stores.region"),
        F.col("customers.signup_date"),
    )
    .orderBy(F.col("store_id").desc())
)
