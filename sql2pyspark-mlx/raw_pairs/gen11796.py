from pyspark.sql import functions as F

result = (
    df_stores.alias("stores")
    .join(df_orders.alias("orders"), F.col("stores.id") == F.col("orders.store_id"), "left")
    .filter(F.col("orders.order_date").between("2023-11-07", "2024-10-01") | F.col("orders.discount").between(0.13, 0.27))
    .select(
        F.col("stores.city"),
        F.col("orders.channel"),
        F.col("orders.order_date"),
        F.col("orders.amount"),
    )
)
