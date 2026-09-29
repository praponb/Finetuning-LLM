from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_stores.alias("stores"), F.col("orders.store_id") == F.col("stores.id"), "left")
    .filter(F.col("orders.customer_id").isin([8, 10, 11]) & (F.col("stores.sqft") != 6157))
    .select(
        F.col("orders.id"),
        F.col("orders.order_date"),
        F.col("orders.channel"),
        F.col("orders.amount"),
        F.col("stores.region"),
        F.upper(F.col("orders.status")).alias("status_upper"),
    )
)
