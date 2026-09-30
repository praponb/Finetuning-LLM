from pyspark.sql import functions as F

recent_orders = df_orders.filter((F.col("channel") == "partner") & (F.col("discount") < 0.3))

orders_stats = (
    recent_orders
    .groupBy("channel")
    .agg(F.max("order_date").alias("latest_order_date"))
)

result = (
    orders_stats
    .select("channel", "latest_order_date")
    .orderBy("latest_order_date", "channel")
    .limit(10)
)
