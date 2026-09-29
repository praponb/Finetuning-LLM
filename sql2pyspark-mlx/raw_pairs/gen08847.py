from pyspark.sql import functions as F

shipments_by_order = (
    df_shipments
    .filter(F.col("order_id").isNotNull())
    .groupBy("order_id")
    .agg(
        F.max("shipped_date").alias("latest_shipped_date"),
        F.round(F.avg("cost"), 2).alias("avg_cost"),
    )
)

result = (
    df_orders.alias("o")
    .join(shipments_by_order.alias("sbo"), F.col("o.id") == F.col("sbo.order_id"), "inner")
    .filter(F.col("sbo.avg_cost") > 33)
    .select(
        F.col("o.shipping_fee"),
        F.col("o.status"),
        F.col("sbo.latest_shipped_date"),
        F.col("sbo.avg_cost"),
    )
    .orderBy("shipping_fee")
)
