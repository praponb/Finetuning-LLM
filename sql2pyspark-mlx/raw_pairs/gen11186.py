from pyspark.sql import functions as F

shipments_by_order = (
    df_shipments
    .filter(F.col("delivered_date") <= "2024-11-01")
    .groupBy("order_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("carrier") == "Flash", 1).otherwise(0))) / F.count("*")), 1).alias("pct_flash"),
        F.sum(F.when(F.col("status") == "LOST", F.col("cost")).otherwise(0)).alias("lost_cost"),
    )
)

result = (
    df_orders.alias("o")
    .join(shipments_by_order.alias("t"), F.col("o.id") == F.col("t.order_id"), "inner")
    .select(
        F.col("o.id"),
        F.col("o.status"),
        F.col("t.pct_flash"),
        F.col("t.lost_cost"),
    )
)
