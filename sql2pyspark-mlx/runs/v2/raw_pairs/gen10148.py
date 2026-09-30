from pyspark.sql import functions as F

shipments_by_order = (
    df_shipments
    .groupBy("order_id")
    .agg(F.min("cost").alias("min_cost"))
)

result = (
    df_orders.alias("o")
    .join(shipments_by_order.alias("sbo"), F.col("o.id") == F.col("sbo.order_id"), "inner")
    .select(F.col("o.id"), F.col("o.amount"), F.col("sbo.min_cost"))
    .orderBy(F.col("id").desc())
)
