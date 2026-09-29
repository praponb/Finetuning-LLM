from pyspark.sql import functions as F

order_summary = (
    df_order_items
    .filter(F.col("quantity") <= 7)
    .groupBy("order_id")
    .agg(
        F.sum("unit_price").alias("total_unit_price"),
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_summary.alias("os"), F.col("o.id") == F.col("os.order_id"), "left")
    .select(
        F.col("o.shipping_fee"),
        F.col("os.total_unit_price"),
        F.col("os.quantity_range"),
    )
    .orderBy(F.col("quantity_range").desc(), F.col("shipping_fee").desc())
)
