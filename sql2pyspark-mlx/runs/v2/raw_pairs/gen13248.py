from pyspark.sql import functions as F

order_summary = (
    df_order_items
    .groupBy("order_id")
    .agg(
        F.max("unit_price").alias("max_unit_price"),
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
    )
)

result = (
    df_orders.alias("orders")
    .join(order_summary.alias("order_summary"), F.col("orders.id") == F.col("order_summary.order_id"), "left")
    .filter(F.col("order_summary.avg_unit_price") > 37)
    .select(
        F.col("orders.store_id"),
        F.col("orders.order_date"),
        F.col("order_summary.max_unit_price"),
        F.col("order_summary.avg_unit_price"),
    )
    .orderBy(F.col("store_id").desc())
)
