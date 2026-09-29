from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .groupBy(
        F.date_format(F.col("o.order_date"), "yyyy-MM").alias("order_month"),
        F.when(F.col("o.channel").isin(["store", "web"]), "group_a").otherwise("other").alias("channel_group"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("s.status")))).alias("status_values"),
    )
    .orderBy(F.col("order_month").desc(), "channel_group")
    .limit(5)
)
