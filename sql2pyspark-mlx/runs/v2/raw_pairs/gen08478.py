from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("status")
    .agg(
        F.count("shipping_fee").alias("shipping_fee_count"),
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
    )
    .filter(F.col("shipping_fee_count") < 5)
    .orderBy("status")
)
