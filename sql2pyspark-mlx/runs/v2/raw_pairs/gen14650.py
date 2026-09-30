from pyspark.sql import functions as F

order_totals = (
    df_shipments
    .filter(F.col("order_id").isNull())
    .groupBy("order_id")
    .agg(F.min("cost").alias("min_cost"))
)

result = (
    df_orders.alias("o")
    .join(order_totals.alias("ot"), F.col("o.id") == F.col("ot.order_id"), "inner")
    .select(F.col("o.id"), F.col("o.order_date"), F.col("ot.min_cost"))
)
