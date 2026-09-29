from pyspark.sql import functions as F

order_summary = (
    df_shipments
    .groupBy("order_id")
    .agg(
        F.round(F.stddev("cost"), 2).alias("stddev_cost"),
        F.max("delivered_date").alias("latest_delivered_date"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_summary.alias("os"), F.col("o.id") == F.col("os.order_id"), "left")
    .filter(F.col("os.stddev_cost") > 33)
    .select(
        F.col("o.id"),
        F.col("o.store_id"),
        F.col("o.shipping_fee"),
        F.col("os.stddev_cost"),
        F.col("os.latest_delivered_date"),
    )
    .orderBy(F.col("store_id").desc())
)
