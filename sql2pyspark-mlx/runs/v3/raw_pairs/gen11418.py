from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .groupBy(
        F.when(F.col("o.channel").isin(["partner", "store"]), "core").otherwise("other").alias("channel_group"),
    )
    .agg(
        F.count(F.col("s.cost")).alias("cost_count"),
        F.sort_array(F.collect_set(F.col("s.status"))).alias("status_list"),
    )
    .orderBy("channel_group")
)
