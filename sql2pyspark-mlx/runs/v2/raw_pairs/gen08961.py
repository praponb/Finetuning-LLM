from pyspark.sql import functions as F

order_totals = (
    df_shipments
    .filter(F.year("shipped_date") >= 2025)
    .groupBy("order_id")
    .agg(F.sort_array(F.collect_set("status")).alias("status_list"))
)

result = (
    df_orders.alias("o")
    .join(order_totals.alias("ot"), F.col("o.id") == F.col("ot.order_id"), "inner")
    .select(F.col("o.order_date"), F.col("ot.status_list"))
    .orderBy(F.col("order_date").desc())
)
