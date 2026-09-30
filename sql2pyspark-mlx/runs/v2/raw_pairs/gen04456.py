from pyspark.sql import functions as F

recent_orders = df_orders.filter(F.col("discount") < 0.29)

agg = (
    recent_orders
    .groupBy("channel")
    .agg(
        F.round(F.avg("shipping_fee"), 2).alias("avg_shipping_fee"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
    )
)

result = (
    agg
    .select("channel", "avg_shipping_fee", "status_list")
    .orderBy(F.col("channel").desc(), F.col("avg_shipping_fee").desc())
)
