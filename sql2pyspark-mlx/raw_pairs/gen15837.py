from pyspark.sql import functions as F

result = (
    df_stores.alias("stores")
    .join(df_orders.alias("orders"), F.col("orders.store_id") == F.col("stores.id"), "inner")
    .select(
        F.col("stores.region"),
        F.col("stores.store_name"),
        F.col("orders.customer_id"),
    )
    .orderBy(F.col("store_name").desc())
)
