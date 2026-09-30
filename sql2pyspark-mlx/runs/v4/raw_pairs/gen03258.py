from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_order_items.alias("oi"), F.col("oi.order_id") == F.col("o.id"), "left")
    .filter(F.col("oi.product_id").isNotNull())
    .groupBy(F.col("o.channel"))
    .agg(
        F.count(F.col("oi.id")).alias("order_item_count"),
        F.min(F.col("oi.quantity")).alias("min_quantity"),
    )
    .filter(F.col("min_quantity") > 3.5)
    .orderBy("channel")
)
