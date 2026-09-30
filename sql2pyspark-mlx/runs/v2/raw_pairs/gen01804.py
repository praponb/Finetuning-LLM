from pyspark.sql import functions as F

order_summary = (
    df_order_items
    .groupBy("order_id")
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        F.sum("unit_price").alias("total_unit_price"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_summary.alias("os"), F.col("o.id") == F.col("os.order_id"), "left")
    .select(
        F.col("o.id"),
        F.col("o.shipping_fee"),
        F.col("o.customer_id"),
        F.col("os.avg_quantity"),
        F.col("os.total_unit_price"),
    )
)
