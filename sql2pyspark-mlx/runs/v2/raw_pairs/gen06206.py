from pyspark.sql import functions as F

shipments_by_order = (
    df_shipments
    .groupBy("order_id")
    .agg(F.sum("cost").alias("total_cost"))
)

result = (
    df_orders.alias("o")
    .join(shipments_by_order.alias("sbo"), F.col("o.id") == F.col("sbo.order_id"), "left")
    .filter(F.col("sbo.total_cost") >= 170)
    .select(
        F.col("o.id"),
        F.col("o.discount"),
        F.col("o.channel"),
        F.col("sbo.total_cost"),
    )
    .orderBy("discount", F.col("channel").desc())
)
