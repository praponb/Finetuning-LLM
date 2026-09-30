from pyspark.sql import functions as F

order_totals = (
    df_order_items
    .groupBy("order_id")
    .agg(
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
        F.countDistinct("unit_price").alias("unique_orders"),
    )
)

result = (
    df_orders.alias("orders")
    .join(order_totals.alias("order_totals"), F.col("orders.id") == F.col("order_totals.order_id"), "inner")
    .select(
        F.col("orders.store_id"),
        F.col("order_totals.quantity_range"),
        F.col("order_totals.unique_orders"),
    )
    .orderBy(F.col("quantity_range").desc())
)
