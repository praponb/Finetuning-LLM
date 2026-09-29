from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_order_items.alias("oi"), F.col("oi.order_id") == F.col("o.id"), "left")
    .filter(F.col("oi.quantity") < 7)
    .select(
        F.col("o.store_id"),
        F.col("o.amount"),
        F.col("o.status"),
        F.col("oi.order_id"),
        F.col("oi.quantity"),
    )
    .orderBy("status")
)
