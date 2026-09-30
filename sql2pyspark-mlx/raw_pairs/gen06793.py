from pyspark.sql import functions as F

order_summary = (
    df_shipments
    .groupBy("order_id")
    .agg(F.max("cost").alias("max_cost"))
)

result = (
    df_orders.alias("o")
    .join(order_summary.alias("O"), F.col("o.id") == F.col("O.order_id"), "left")
    .filter(F.col("O.max_cost") >= 69)
    .select(F.col("o.id"), F.col("o.discount"), F.col("O.max_cost"))
)
