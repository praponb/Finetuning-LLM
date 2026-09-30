from pyspark.sql import functions as F

order_stats = (
    df_shipments
    .filter(~F.col("status").isin(["RETURNED", "DELIVERED"]))
    .groupBy("order_id")
    .agg(F.countDistinct("cost").alias("unique_carriers"))
)

result = (
    df_orders.alias("o")
    .join(order_stats.alias("os"), F.col("o.id") == F.col("os.order_id"), "left")
    .select(F.col("o.store_id"), F.col("o.customer_id"), F.col("os.unique_carriers"))
)
