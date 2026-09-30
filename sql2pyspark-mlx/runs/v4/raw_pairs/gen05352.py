from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("channel")
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.round(F.avg("shipping_fee"), 2).alias("avg_shipping_fee"),
        F.sum("discount").alias("total_discount"),
    )
    .filter(F.col("avg_shipping_fee") >= 28)
    .orderBy("channel")
)
