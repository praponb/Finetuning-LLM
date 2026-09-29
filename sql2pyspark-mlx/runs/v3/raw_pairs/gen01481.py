from pyspark.sql import functions as F

order_totals = (
    df_order_items
    .groupBy("order_id")
    .agg(
        F.count("unit_price").alias("unit_price_count"),
        F.sum("quantity").alias("total_quantity"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_totals.alias("ot"), F.col("o.id") == F.col("ot.order_id"), "inner")
    .filter(F.col("ot.total_quantity") >= 41)
    .select(
        F.col("o.amount"),
        F.col("o.status"),
        F.col("ot.unit_price_count"),
        F.col("ot.total_quantity"),
    )
    .orderBy("amount")
)
