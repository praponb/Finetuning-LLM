from pyspark.sql import functions as F

order_totals = (
    df_shipments
    .filter(F.col("status").isin(["DELIVERED", "RETURNED"]))
    .groupBy("order_id")
    .agg(F.round(F.avg("cost"), 2).alias("avg_cost"))
)

result = (
    df_orders.alias("o")
    .join(order_totals.alias("ot"), F.col("o.id") == F.col("ot.order_id"), "left")
    .filter(F.col("ot.avg_cost") >= 55)
    .select(
        F.col("o.id"),
        F.col("o.store_id"),
        F.col("o.channel"),
        F.col("ot.avg_cost"),
    )
    .orderBy(F.col("avg_cost").desc())
)
