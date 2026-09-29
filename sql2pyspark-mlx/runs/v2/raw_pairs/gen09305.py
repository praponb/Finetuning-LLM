from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .groupBy(
        F.date_trunc("MONTH", F.col("o.order_date")).alias("order_month"),
        F.col("o.status"),
    )
    .agg(
        F.round(F.stddev(F.col("oi.quantity")), 2).alias("stddev_quantity"),
        F.count(F.col("oi.quantity")).alias("quantity_count"),
    )
    .filter(F.col("quantity_count") > 5)
    .orderBy("order_month", "stddev_quantity", "status")
    .limit(5)
)
