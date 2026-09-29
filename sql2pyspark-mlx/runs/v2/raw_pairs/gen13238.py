from pyspark.sql import functions as F

order_summary = (
    df_order_items
    .groupBy("order_id")
    .agg(F.count("unit_price").alias("unit_price_count"))
)

result = (
    df_orders.alias("orders")
    .join(order_summary.alias("t"), F.col("orders.id") == F.col("t.order_id"), "left")
    .filter(F.col("t.unit_price_count") > 5)
    .select(
        F.col("orders.id"),
        F.col("orders.amount"),
        F.col("orders.channel"),
        F.col("t.unit_price_count"),
    )
    .orderBy("channel")
)
