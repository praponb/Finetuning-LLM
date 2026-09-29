from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_order_items.filter(F.col("quantity") < 6).alias("oi"), F.col("o.id") == F.col("oi.order_id"), "left_anti")
    .select(F.col("o.id"), F.col("o.store_id"), F.col("o.channel"), F.col("o.amount"))
)
