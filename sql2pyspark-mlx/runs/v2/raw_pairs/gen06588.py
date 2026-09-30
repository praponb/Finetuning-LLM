from pyspark.sql import functions as F

order_totals = (
    df_order_items
    .filter(F.col("order_id").isin([5, 29, 38, 40]))
    .groupBy("order_id")
    .agg(
        F.max("unit_price").alias("max_unit_price"),
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_totals.alias("ot"), F.col("o.id") == F.col("ot.order_id"), "inner")
    .select(
        F.col("o.id"),
        F.col("o.customer_id"),
        F.col("o.discount"),
        F.col("ot.max_unit_price"),
        F.col("ot.quantity_range"),
    )
    .orderBy("customer_id", "id")
)
