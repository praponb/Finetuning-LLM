from pyspark.sql import functions as F

order_summary = (
    df_shipments
    .groupBy("order_id")
    .agg(F.sort_array(F.collect_set("status")).alias("status_list"))
)

result = (
    df_orders.alias("orders")
    .join(order_summary.alias("order_summary"), F.col("orders.id") == F.col("order_summary.order_id"), "inner")
    .select(
        F.col("orders.id"),
        F.col("orders.order_date"),
        F.col("orders.channel"),
        F.col("order_summary.status_list"),
    )
    .orderBy(F.col("order_date").desc(), "id")
)
