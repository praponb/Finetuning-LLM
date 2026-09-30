from pyspark.sql import functions as F

shipments_by_order = (
    df_shipments
    .filter(F.col("delivered_date") < "2023-11-04")
    .groupBy("order_id")
    .agg(
        F.sum("cost").alias("total_cost"),
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
)

result = (
    df_orders.alias("o")
    .join(shipments_by_order.alias("sbo"), F.col("o.id") == F.col("sbo.order_id"), "inner")
    .select(
        F.col("o.id"),
        F.col("o.channel"),
        F.col("sbo.total_cost"),
        F.col("sbo.status_values"),
    )
    .orderBy(F.col("channel").desc(), "id")
    .limit(3)
)
