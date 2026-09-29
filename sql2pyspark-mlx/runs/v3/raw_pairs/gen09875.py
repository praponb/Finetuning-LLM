from pyspark.sql import functions as F

order_totals = (
    df_shipments
    .filter(F.col("order_id") == 28)
    .groupBy("order_id")
    .agg(F.sum("cost").alias("total_cost"))
)

result = (
    df_orders.alias("o")
    .join(order_totals.alias("ot"), F.col("o.id") == F.col("ot.order_id"), "inner")
    .select(F.col("o.id"), F.col("o.customer_id"), F.col("ot.total_cost"))
    .orderBy(F.col("id").desc(), F.col("customer_id").desc())
)
