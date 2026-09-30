from pyspark.sql import functions as F

result = (
    df_orders
    .agg(
        F.countDistinct("shipping_fee").alias("unique_channels"),
        F.max("order_date").alias("latest_order_date"),
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
    )
    .orderBy(F.col("unique_channels").desc())
)
