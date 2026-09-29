from pyspark.sql import functions as F

order_stats = (
    df_shipments
    .filter(F.col("status") != "LOST")
    .groupBy("order_id")
    .agg(F.min("cost").alias("min_cost"))
)

result = (
    df_orders.alias("o")
    .join(order_stats.alias("os"), F.col("o.id") == F.col("os.order_id"), "inner")
    .select(F.col("o.id"), F.col("o.channel"), F.col("os.min_cost"))
    .orderBy("id")
)
