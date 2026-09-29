from pyspark.sql import functions as F

order_totals = (
    df_shipments
    .groupBy("order_id")
    .agg(F.sum("cost").alias("total_cost"))
)

result = (
    df_orders.alias("o")
    .join(order_totals.alias("ot"), F.col("o.id") == F.col("ot.order_id"), "inner")
    .filter(F.col("ot.total_cost") > 530)
    .select(F.col("o.order_date"), F.col("o.customer_id"), F.col("ot.total_cost"))
)
