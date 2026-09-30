from pyspark.sql import functions as F

shipments_by_order = (
    df_shipments
    .groupBy("order_id")
    .agg(
        F.max("delivered_date").alias("latest_delivered_date"),
        F.avg("cost").alias("avg_cost"),
    )
)

result = (
    df_orders.alias("o")
    .join(shipments_by_order.alias("sbo"), F.col("o.id") == F.col("sbo.order_id"), "inner")
    .filter(F.col("sbo.avg_cost") >= 37)
    .select(
        F.col("o.shipping_fee"),
        F.col("o.channel"),
        F.col("sbo.latest_delivered_date"),
        F.col("sbo.avg_cost"),
    )
)
