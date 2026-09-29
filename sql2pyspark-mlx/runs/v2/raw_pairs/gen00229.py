from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_order_items.alias("oi"), F.col("o.id") == F.col("oi.order_id"), "left")
    .filter(F.col("o.channel") == "mobile")
    .groupBy(F.col("o.channel"))
    .agg(F.count(F.col("oi.id")).alias("order_item_count"))
    .filter(F.col("order_item_count") >= 3)
    .orderBy("channel")
    .limit(10)
)
