from pyspark.sql import functions as F

shipments_by_order = (
    df_shipments
    .filter(F.col("cost") >= 80)
    .groupBy("order_id")
    .agg(
        F.count(F.when(F.col("status") == "DELIVERED", 1)).alias("delivered_count"),
        F.sum(F.when(F.col("status") == "DELIVERED", F.col("cost")).otherwise(0)).alias("delivered_cost"),
    )
)

result = (
    df_orders.alias("o")
    .join(shipments_by_order.alias("sbo"), F.col("o.id") == F.col("sbo.order_id"), "inner")
    .select(
        F.col("o.shipping_fee"),
        F.col("o.customer_id"),
        F.col("sbo.delivered_count"),
        F.col("sbo.delivered_cost"),
    )
)
