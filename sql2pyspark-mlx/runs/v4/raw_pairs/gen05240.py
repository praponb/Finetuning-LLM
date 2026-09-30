from pyspark.sql import functions as F

order_stats = (
    df_shipments
    .filter(F.col("order_id").isNull())
    .groupBy("order_id")
    .agg(F.max("delivered_date").alias("last_delivered_date"))
)

result = (
    df_orders.alias("orders")
    .join(order_stats.alias("OrderStats"), F.col("orders.id") == F.col("OrderStats.order_id"), "inner")
    .select(
        F.col("orders.order_date"),
        F.col("orders.customer_id"),
        F.col("OrderStats.last_delivered_date"),
    )
    .orderBy("order_date")
)
