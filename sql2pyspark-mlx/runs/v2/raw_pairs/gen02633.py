from pyspark.sql import functions as F

order_totals = (
    df_shipments
    .groupBy("order_id")
    .agg(
        F.max("shipped_date").alias("last_shipped_date"),
        F.min("delivered_date").alias("first_delivered_date"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_totals.alias("t"), F.col("o.id") == F.col("t.order_id"), "left")
    .select(
        F.col("o.id"),
        F.col("o.order_date"),
        F.col("o.discount"),
        F.col("t.last_shipped_date"),
        F.col("t.first_delivered_date"),
    )
    .orderBy("order_date")
)
