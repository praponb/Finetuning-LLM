from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(
        F.when(F.col("shipping_fee") >= 38, "high").when(F.col("shipping_fee") >= 17, "mid").otherwise("low").alias("shipping_fee_bucket"),
    )
    .agg(
        F.countDistinct("shipping_fee").alias("unique_channels"),
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
    )
    .orderBy("shipping_fee_bucket")
)
