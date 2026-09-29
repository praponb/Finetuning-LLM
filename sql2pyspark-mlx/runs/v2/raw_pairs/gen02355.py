from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("store_id", "channel", "status")
    .agg(
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
        F.max("order_date").alias("latest_order_date"),
        F.sum(F.when(F.col("status") == "COMPLETED", F.col("shipping_fee")).otherwise(0)).alias("completed_shipping_fee"),
    )
    .filter(F.col("completed_shipping_fee") < 62)
)
