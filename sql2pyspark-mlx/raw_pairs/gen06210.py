from pyspark.sql import functions as F

result = (
    df_payments.alias("payments")
    .join(df_orders.alias("orders"), F.col("payments.order_id") == F.col("orders.id"), "left")
    .join(df_stores.alias("stores"), F.col("orders.store_id") == F.col("stores.id"), "left")
    .filter(F.col("orders.order_date") <= "2025-09-01")
    .select(
        F.col("payments.amount"),
        F.col("payments.status"),
        F.col("orders.channel"),
        F.col("orders.store_id"),
        F.col("stores.opened_date"),
    )
)
