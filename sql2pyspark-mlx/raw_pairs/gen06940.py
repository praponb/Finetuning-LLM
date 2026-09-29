from pyspark.sql import functions as F

order_totals = (
    df_shipments
    .groupBy("order_id")
    .agg(F.sum("cost").alias("total_cost"))
)

result = (
    df_orders.alias("o")
    .join(order_totals.alias("O"), F.col("o.id") == F.col("O.order_id"), "inner")
    .filter(F.col("O.total_cost") >= 530)
    .select(F.col("o.discount"), F.col("o.channel"), F.col("O.total_cost"))
)
