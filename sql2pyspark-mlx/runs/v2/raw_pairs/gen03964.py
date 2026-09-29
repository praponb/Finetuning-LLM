from pyspark.sql import functions as F

order_totals = (
    df_order_items
    .groupBy("order_id")
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_totals.alias("ot"), F.col("o.id") == F.col("ot.order_id"), "left")
    .select(
        F.col("o.id"),
        F.col("o.order_date"),
        F.col("o.store_id"),
        F.col("ot.avg_quantity"),
        F.col("ot.avg_unit_price"),
    )
)
