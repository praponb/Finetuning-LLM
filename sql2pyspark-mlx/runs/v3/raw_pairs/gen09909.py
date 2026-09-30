from pyspark.sql import functions as F

order_summary = (
    df_shipments
    .groupBy("order_id")
    .agg(
        F.round(F.stddev("cost"), 2).alias("stddev_cost"),
        F.count("*").alias("shipment_count"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_summary.alias("os"), F.col("o.id") == F.col("os.order_id"), "inner")
    .filter(F.col("os.shipment_count") >= 8)
    .select(
        F.col("o.id"),
        F.col("o.customer_id"),
        F.col("o.shipping_fee"),
        F.col("os.stddev_cost"),
        F.col("os.shipment_count"),
    )
    .orderBy(F.col("shipping_fee").desc(), F.col("customer_id").desc())
)
