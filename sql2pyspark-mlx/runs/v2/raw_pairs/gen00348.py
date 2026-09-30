from pyspark.sql import functions as F

filtered_orders = (
    df_orders
    .filter(~((F.col("amount").between(1200, 1290) & ~F.col("channel").isin(["web", "store"])) | (F.col("customer_id") == 18)))
    .withColumn("channel_status", F.concat("channel", F.lit(" "), "status"))
)

orders_stats = (
    filtered_orders
    .groupBy(F.quarter("order_date").alias("order_quarter"), "channel")
    .agg(
        F.round(F.stddev("discount"), 2).alias("stddev_discount"),
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
    )
)

result = (
    orders_stats
    .select("order_quarter", "channel", "stddev_discount", "channel_list")
    .orderBy(F.col("stddev_discount").desc(), F.col("channel").desc())
)
