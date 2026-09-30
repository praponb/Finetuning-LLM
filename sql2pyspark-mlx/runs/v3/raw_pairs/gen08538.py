from pyspark.sql import functions as F

order_totals = (
    df_shipments
    .filter(F.col("carrier") == "DHL")
    .groupBy("order_id")
    .agg(
        F.sum(F.when(F.col("status") == "DELIVERED", 1).otherwise(0)).alias("delivered_count"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_totals.alias("t"), F.col("o.id") == F.col("t.order_id"), "left")
    .select(
        F.col("o.id"),
        F.col("o.store_id"),
        F.col("o.status"),
        F.col("t.delivered_count"),
    )
    .orderBy(F.col("store_id").desc(), "id")
)
