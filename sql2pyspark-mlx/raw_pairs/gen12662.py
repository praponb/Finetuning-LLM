from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("channel", "status")
    .agg(
        F.countDistinct("amount").alias("unique_statuss"),
        F.round(F.avg("shipping_fee"), 2).alias("avg_shipping_fee"),
        F.concat_ws(", ", F.sort_array(F.collect_set("channel"))).alias("channel_values"),
    )
    .filter(F.col("unique_statuss") < 1)
    .orderBy(F.col("avg_shipping_fee").desc())
)
