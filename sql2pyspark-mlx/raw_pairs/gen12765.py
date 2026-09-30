from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_order_items.alias("order_items"), F.col("orders.id") == F.col("order_items.order_id"), "left_semi")
    .filter(~F.col("orders.channel").isin(["mobile", "web"]))
    .select(
        F.col("orders.id"),
        F.col("orders.shipping_fee"),
        F.col("orders.store_id"),
        F.col("orders.channel"),
    )
    .orderBy("channel", "id")
    .limit(50)
)
