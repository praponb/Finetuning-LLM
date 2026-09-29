from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_order_items.alias("oi"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .groupBy(F.col("o.id"))
    .agg(F.count(F.col("oi.id")).alias("order_item_count"))
    .orderBy(F.col("id").desc(), "order_item_count")
)
