from pyspark.sql import functions as F

valid_orders = (
    df_orders
    .filter(~(F.col("customer_id").isNull() & (F.col("channel") == "mobile") & (F.col("shipping_fee") > 23)))
)

orders_stats = (
    valid_orders
    .groupBy(F.quarter("order_date").alias("order_quarter"), "status")
    .agg(F.sort_array(F.collect_set("channel")).alias("channel_list"))
)

result = (
    orders_stats
    .select("order_quarter", "status", "channel_list")
    .orderBy("status")
)
