from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_orders.alias("orders"), F.col("order_items.order_id") == F.col("orders.id"), "right")
    .join(df_customers.alias("customers"), F.col("customers.id") == F.col("orders.customer_id"), "left")
    .groupBy(
        F.when(F.col("orders.channel").isin(["store", "mobile"]), "group_a").otherwise("other").alias("channel_group"),
    )
    .agg(F.countDistinct(F.col("order_items.quantity")).alias("unique_orders"))
    .filter(F.col("unique_orders") >= 1)
    .orderBy("channel_group", F.col("unique_orders").desc())
)
