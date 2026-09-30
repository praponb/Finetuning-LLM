from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_orders.alias("orders"), F.col("order_items.order_id") == F.col("orders.id"), "left")
    .filter(F.col("orders.store_id") == 10)
    .groupBy(F.col("orders.channel"))
    .agg(
        F.sum((F.col("order_items.quantity") * F.col("order_items.unit_price"))).alias("revenue"),
    )
    .orderBy("channel")
    .limit(5)
)
