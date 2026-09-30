from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_order_items.filter(F.col("quantity") <= 8).alias("order_items"), F.col("orders.id") == F.col("order_items.order_id"), "left_anti")
    .select(
        F.col("orders.id"),
        F.col("orders.order_date"),
        F.col("orders.channel"),
        F.col("orders.discount"),
    )
)
