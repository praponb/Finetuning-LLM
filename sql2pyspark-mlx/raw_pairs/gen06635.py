from pyspark.sql import functions as F

order_summary = (
    df_order_items
    .groupBy("order_id")
    .agg((F.max("quantity") - F.min("quantity")).alias("quantity_range"))
)

result = (
    df_orders.alias("o")
    .join(order_summary.alias("O"), F.col("o.id") == F.col("O.order_id"), "inner")
    .filter(F.col("O.quantity_range") >= 47)
    .select(
        F.col("o.id"),
        F.col("o.discount"),
        F.col("o.shipping_fee"),
        F.col("O.quantity_range"),
    )
    .orderBy(F.col("quantity_range").desc())
)
