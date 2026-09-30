from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_stores.alias("stores"), F.col("orders.store_id") == F.col("stores.id"), "inner")
    .select(
        F.col("orders.id"),
        F.col("orders.store_id"),
        F.col("orders.amount"),
        F.col("orders.customer_id"),
        F.col("stores.sqft"),
        F.col("stores.region"),
    )
)
