from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_order_items.alias("oi"), F.col("o.id") == F.col("oi.order_id"), "left")
    .filter(F.col("oi.quantity") > 8)
    .groupBy(F.col("o.channel"))
    .agg(
        F.count(F.col("oi.id")).alias("order_item_count"),
        F.round(F.avg(F.col("oi.unit_price")), 2).alias("avg_unit_price"),
        F.sum(F.col("oi.quantity")).alias("total_quantity"),
    )
    .orderBy(F.col("order_item_count").desc(), "channel")
    .limit(1)
)
