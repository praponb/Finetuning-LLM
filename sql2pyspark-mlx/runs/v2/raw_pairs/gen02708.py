from pyspark.sql import functions as F

result = (
    df_shipments.alias("shipments")
    .join(df_orders.alias("orders"), F.col("shipments.order_id") == F.col("orders.id"), "outer")
    .join(df_stores.alias("stores"), F.col("orders.store_id") == F.col("stores.id"), "left")
    .groupBy(F.month(F.col("stores.opened_date")).alias("opened_month"))
    .agg(
        F.sum(F.col("shipments.cost")).alias("total_cost"),
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("shipments.status")))).alias("status_values"),
    )
)
