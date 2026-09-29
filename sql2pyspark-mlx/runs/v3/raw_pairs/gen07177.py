from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_stores.alias("stores"), F.col("orders.store_id") == F.col("stores.id"), "left")
    .filter((F.col("stores.city") != "Singapore") & ~F.col("stores.region").isin(["West", "North", "Central"]))
    .select(
        F.col("orders.id"),
        F.col("orders.customer_id"),
        F.col("orders.channel"),
        F.col("stores.store_name"),
        F.col("stores.city"),
    )
    .orderBy("city")
)
