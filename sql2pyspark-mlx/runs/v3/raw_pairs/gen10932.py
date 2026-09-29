from pyspark.sql import functions as F

order_stats = (
    df_shipments
    .filter(F.col("cost") < 65)
    .groupBy("order_id")
    .agg(F.max("shipped_date").alias("last_shipped_date"))
)

result = (
    df_orders.alias("o")
    .join(order_stats.alias("os"), F.col("o.id") == F.col("os.order_id"), "left")
    .select(F.col("o.shipping_fee"), F.col("o.status"), F.col("os.last_shipped_date"))
)
