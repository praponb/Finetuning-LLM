from pyspark.sql import functions as F

order_totals = (
    df_order_items
    .groupBy("order_id")
    .agg(
        F.countDistinct("unit_price").alias("unique_orders"),
        F.avg("quantity").alias("avg_quantity"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_totals.alias("ot"), F.col("o.id") == F.col("ot.order_id"), "inner")
    .select(F.col("o.customer_id"), F.col("ot.unique_orders"), F.col("ot.avg_quantity"))
    .orderBy(F.col("customer_id").desc())
)
