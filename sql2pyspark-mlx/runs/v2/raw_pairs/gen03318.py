from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_order_items.alias("order_items"), F.col("orders.id") == F.col("order_items.order_id"), "left")
    .groupBy(F.col("orders.id"), F.col("orders.channel"))
    .agg(
        F.count(F.col("order_items.id")).alias("order_item_count"),
        F.round(F.stddev(F.col("order_items.quantity")), 2).alias("stddev_quantity"),
    )
    .filter(F.col("stddev_quantity") >= 66)
    .orderBy(F.col("channel").desc(), "id")
    .limit(10)
)
