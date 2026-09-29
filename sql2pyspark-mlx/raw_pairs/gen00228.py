from pyspark.sql import functions as F

shipments_by_order = (
    df_shipments
    .groupBy("order_id")
    .agg(
        F.min("delivered_date").alias("min_delivered_date"),
        F.count("cost").alias("cost_count"),
    )
)

result = (
    df_orders.alias("o")
    .join(shipments_by_order.alias("S"), F.col("o.id") == F.col("S.order_id"), "inner")
    .filter(F.col("S.cost_count") >= 7)
    .select(
        F.col("o.id"),
        F.col("o.channel"),
        F.col("S.min_delivered_date"),
        F.col("S.cost_count"),
    )
    .orderBy(F.col("cost_count").desc(), F.col("id").desc())
    .limit(20)
)
