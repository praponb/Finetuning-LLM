from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(F.quarter("order_date").alias("order_quarter"))
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
        F.round(F.avg("discount"), 2).alias("avg_discount"),
    )
    .orderBy("avg_discount")
)
