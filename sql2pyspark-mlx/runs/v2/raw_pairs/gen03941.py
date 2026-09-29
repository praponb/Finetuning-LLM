from pyspark.sql import functions as F

shipments_by_order = (
    df_shipments
    .groupBy("order_id")
    .agg(
        F.min("shipped_date").alias("min_shipped_date"),
        F.avg("cost").alias("avg_cost"),
    )
)

result = (
    df_orders.alias("o")
    .join(shipments_by_order.alias("sbo"), F.col("o.id") == F.col("sbo.order_id"), "inner")
    .filter(F.col("sbo.avg_cost") > 55)
    .select(
        F.col("o.id"),
        F.col("o.amount"),
        F.col("o.order_date"),
        F.col("sbo.min_shipped_date"),
        F.col("sbo.avg_cost"),
    )
    .orderBy(F.col("min_shipped_date").desc())
)
