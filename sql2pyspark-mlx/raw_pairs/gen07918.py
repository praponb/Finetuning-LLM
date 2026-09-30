from pyspark.sql import functions as F

order_summary = (
    df_order_items
    .filter(F.col("unit_price") <= 110)
    .groupBy("order_id")
    .agg(
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
        F.count("*").alias("num_order_items"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_summary.alias("os"), F.col("o.id") == F.col("os.order_id"), "left")
    .filter(F.col("os.num_order_items") >= 7)
    .select(
        F.col("o.status"),
        F.col("o.shipping_fee"),
        F.col("os.stddev_quantity"),
        F.col("os.num_order_items"),
    )
)
