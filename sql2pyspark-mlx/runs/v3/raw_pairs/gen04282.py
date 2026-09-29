from pyspark.sql import functions as F

order_stats = (
    df_shipments
    .filter(F.col("cost") < 32)
    .groupBy("order_id")
    .agg(F.sort_array(F.collect_set("carrier")).alias("carrier_list"))
)

result = (
    df_orders.alias("o")
    .join(order_stats.alias("os"), F.col("o.id") == F.col("os.order_id"), "inner")
    .select(F.col("o.channel"), F.col("os.carrier_list"))
)
