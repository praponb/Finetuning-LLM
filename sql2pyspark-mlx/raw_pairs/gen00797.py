from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_stores.alias("stores"), F.col("orders.store_id") == F.col("stores.id"), "inner")
    .join(df_customers.alias("customers"), F.col("orders.customer_id") == F.col("customers.id"), "inner")
    .filter(F.col("customers.city") != "Berlin")
    .select(
        F.col("orders.id"),
        F.col("orders.customer_id"),
        F.col("stores.region"),
        F.col("customers.email"),
        F.col("customers.segment"),
        F.initcap(F.trim(F.col("customers.segment"))).alias("segment_title"),
    )
)
