from pyspark.sql import functions as F

result = (
    df_payments.alias("payments")
    .join(df_orders.alias("orders"), F.col("payments.order_id") == F.col("orders.id"), "right")
    .join(df_stores.alias("stores"), F.col("orders.store_id") == F.col("stores.id"), "left")
    .select(
        F.col("payments.order_id"),
        F.col("orders.channel"),
        F.col("orders.status"),
        F.col("stores.store_name"),
    )
    .orderBy("status")
)
