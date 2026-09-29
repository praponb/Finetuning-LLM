from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("channel") == "web")
    .groupBy("channel")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("channel"))).alias("channel_values"),
        F.round(F.avg("shipping_fee"), 2).alias("avg_shipping_fee"),
    )
    .filter(F.col("avg_shipping_fee") < 43)
    .orderBy("channel")
)
