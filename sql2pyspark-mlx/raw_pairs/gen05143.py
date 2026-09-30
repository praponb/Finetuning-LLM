from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_order_items.alias("oi"), F.col("oi.order_id") == F.col("o.id"), "left")
    .groupBy(F.col("o.id"), F.col("o.channel"))
    .agg(
        F.count(F.col("oi.id")).alias("order_item_count"),
        F.count(F.col("oi.quantity")).alias("quantity_count"),
    )
    .orderBy("channel")
)
