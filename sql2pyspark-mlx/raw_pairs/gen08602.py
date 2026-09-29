from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "right")
    .filter(F.col("o.customer_id").isin([7, 12, 27, 40]))
    .groupBy(F.col("o.store_id"))
    .agg(
        F.round(F.stddev(F.col("oi.unit_price")), 2).alias("stddev_unit_price"),
        F.count(F.col("oi.unit_price")).alias("unit_price_count"),
    )
    .orderBy("store_id")
)
