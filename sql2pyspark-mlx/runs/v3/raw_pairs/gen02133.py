from pyspark.sql import functions as F

valid_orders = (
    df_orders
    .filter((F.col("status") == "PENDING") & F.month("order_date").isin([7, 10, 12]))
    .withColumn("channel_title", F.initcap(F.trim("channel")))
)

orders_stats = (
    valid_orders
    .groupBy("channel")
    .agg(F.sort_array(F.collect_set("status")).alias("status_list"))
)

result = orders_stats
