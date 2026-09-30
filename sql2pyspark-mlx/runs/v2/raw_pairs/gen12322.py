from pyspark.sql import functions as F

order_totals = (
    df_order_items
    .groupBy("order_id")
    .agg(
        F.max("quantity").alias("max_quantity"),
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_totals.alias("ot"), F.col("o.id") == F.col("ot.order_id"), "inner")
    .filter(F.col("ot.max_quantity") > 4.6)
    .select(
        F.col("o.id"),
        F.col("o.customer_id"),
        F.col("ot.max_quantity"),
        F.col("ot.avg_unit_price"),
    )
    .orderBy(F.col("id").desc())
)
