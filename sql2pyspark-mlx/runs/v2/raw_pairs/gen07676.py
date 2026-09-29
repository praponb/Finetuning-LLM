from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_order_items.alias("oi"), F.col("o.id") == F.col("oi.order_id"), "left")
    .select(F.col("o.amount"), F.col("o.customer_id"), F.col("oi.order_id"))
)
