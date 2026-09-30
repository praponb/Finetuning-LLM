from pyspark.sql import functions as F

order_totals = (
    df_order_items
    .groupBy("order_id")
    .agg(F.round(F.avg("quantity"), 2).alias("avg_quantity"))
)

result = (
    df_orders.alias("orders")
    .join(order_totals.alias("t"), F.col("orders.id") == F.col("t.order_id"), "inner")
    .select(
        F.col("orders.id"),
        F.col("orders.discount"),
        F.col("orders.customer_id"),
        F.col("t.avg_quantity"),
    )
    .orderBy(F.col("discount").desc())
)
