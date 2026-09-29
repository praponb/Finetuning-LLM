from pyspark.sql import functions as F

recent_orders = df_orders.filter(F.col("channel") == "web")

orders_stats = (
    recent_orders
    .groupBy("channel", "status")
    .agg(F.countDistinct("shipping_fee").alias("unique_channels"))
)

result = (
    orders_stats
    .filter(F.col("unique_channels") > 8)
    .select("channel", "status", "unique_channels")
)
