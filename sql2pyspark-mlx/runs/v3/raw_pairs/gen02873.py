from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("status")
    .agg(
        F.round(F.avg("discount"), 2).alias("avg_discount"),
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
        F.min("order_date").alias("min_order_date"),
    )
)
