from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("discount") < 0.35)
    .agg(
        (F.max("shipping_fee") - F.min("shipping_fee")).alias("shipping_fee_range"),
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
        F.sum("shipping_fee").alias("total_shipping_fee"),
    )
)
