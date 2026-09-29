from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_stores.alias("stores"), F.col("stores.id") == F.col("orders.store_id"), "inner")
    .join(df_customers.alias("customers"), F.col("orders.customer_id") == F.col("customers.id"), "inner")
    .select(
        F.col("orders.id"),
        F.col("orders.discount"),
        F.col("stores.opened_date"),
        F.col("customers.segment"),
        F.lower(F.col("stores.region")).alias("region_lower"),
    )
    .orderBy("region_lower", "id")
    .limit(10)
)
