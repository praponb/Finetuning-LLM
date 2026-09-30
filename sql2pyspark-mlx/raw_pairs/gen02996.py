from pyspark.sql import functions as F

order_totals = (
    df_shipments
    .groupBy("order_id")
    .agg(F.count("cost").alias("cost_count"))
)

result = (
    df_orders.alias("o")
    .join(order_totals.alias("ot"), F.col("o.id") == F.col("ot.order_id"), "left")
    .filter(F.col("ot.cost_count") > 8)
    .select(F.col("o.status"), F.col("ot.cost_count"))
    .orderBy(F.col("status").desc())
)
