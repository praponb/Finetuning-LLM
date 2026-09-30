from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "right")
    .filter(F.col("oi.quantity") < 4)
    .select(F.col("oi.quantity"), F.col("oi.order_id"), F.col("o.amount"))
    .orderBy("quantity")
)
