from pyspark.sql import functions as F

order_totals = (
    df_shipments
    .filter(F.col("delivered_date") < "2023-10-01")
    .groupBy("order_id")
    .agg(
        F.sum(F.when(F.col("status") == "LOST", F.col("cost")).otherwise(0)).alias("lost_cost"),
    )
)

result = (
    df_orders.alias("orders")
    .join(order_totals.alias("t"), F.col("orders.id") == F.col("t.order_id"), "inner")
    .select(F.col("orders.id"), F.col("orders.store_id"), F.col("t.lost_cost"))
    .orderBy(F.col("lost_cost").desc(), "id")
    .limit(1)
)
