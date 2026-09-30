from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_customers.alias("customers"), F.col("orders.customer_id") == F.col("customers.id"), "left")
    .join(df_stores.alias("stores"), F.col("orders.store_id") == F.col("stores.id"), "inner")
    .filter(F.lower(F.trim(F.col("stores.store_name"))) == "old town")
    .select(
        F.col("orders.status"),
        F.col("customers.signup_date"),
        F.col("customers.segment"),
        F.col("stores.opened_date"),
    )
    .orderBy(F.col("opened_date").desc(), "signup_date")
)
