from pyspark.sql import functions as F

order_totals = (
    df_order_items
    .groupBy("order_id")
    .agg((F.max("quantity") - F.min("quantity")).alias("quantity_range"))
)

result = (
    df_orders.alias("orders")
    .join(order_totals.alias("order_totals"), F.col("orders.id") == F.col("order_totals.order_id"), "inner")
    .select(
        F.col("orders.id"),
        F.col("orders.store_id"),
        F.col("orders.order_date"),
        F.col("order_totals.quantity_range"),
    )
    .orderBy("store_id", "id")
    .limit(5)
)
