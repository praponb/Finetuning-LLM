from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .filter(F.col("o.shipping_fee").isNull())
    .groupBy(F.col("o.channel"))
    .agg(
        F.round(F.stddev(F.col("s.cost")), 2).alias("stddev_cost"),
        F.max(F.col("s.cost")).alias("max_cost"),
    )
    .orderBy("stddev_cost", "channel")
    .limit(5)
)
