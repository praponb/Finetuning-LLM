from pyspark.sql import functions as F

order_totals = (
    df_shipments
    .filter(F.col("order_id").isNotNull())
    .groupBy("order_id")
    .agg(F.sort_array(F.collect_set("status")).alias("status_list"))
)

result = (
    df_orders.alias("orders")
    .join(order_totals.alias("t"), F.col("orders.id") == F.col("t.order_id"), "left")
    .select(
        F.col("orders.id"),
        F.col("orders.shipping_fee"),
        F.col("orders.status"),
        F.col("t.status_list"),
    )
    .orderBy(F.col("shipping_fee").desc(), "status")
)
