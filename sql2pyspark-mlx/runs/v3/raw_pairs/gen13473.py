from pyspark.sql import functions as F

order_summary = (
    df_order_items
    .groupBy("order_id")
    .agg(
        F.count("*").alias("num_order_items"),
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_summary.alias("os"), F.col("o.id") == F.col("os.order_id"), "left")
    .select(
        F.col("o.customer_id"),
        F.col("o.order_date"),
        F.col("os.num_order_items"),
        F.col("os.stddev_quantity"),
    )
)
