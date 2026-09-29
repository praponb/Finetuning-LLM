from pyspark.sql import functions as F

shipments_by_order = (
    df_shipments
    .groupBy("order_id")
    .agg(
        F.sum(F.when(F.col("status") == "DELIVERED", 1).otherwise(0)).alias("delivered_count"),
        F.round(((100 * F.sum(F.when(F.col("status") == "IN_TRANSIT", 1).otherwise(0))) / F.count("*")), 1).alias("pct_in_transit"),
    )
)

result = (
    df_orders.alias("o")
    .join(shipments_by_order.alias("sbo"), F.col("o.id") == F.col("sbo.order_id"), "inner")
    .filter(F.col("sbo.pct_in_transit") > 79)
    .select(
        F.col("o.id"),
        F.col("o.store_id"),
        F.col("o.discount"),
        F.col("sbo.delivered_count"),
        F.col("sbo.pct_in_transit"),
    )
)
