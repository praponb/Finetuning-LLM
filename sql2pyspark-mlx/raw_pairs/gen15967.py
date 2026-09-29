from pyspark.sql import functions as F

recent_orders = (
    df_orders
    .filter(F.col("channel").isin(["partner", "store", "mobile"]))
    .withColumn("weeks_since_order_date", (F.datediff(F.current_date(), "order_date") / 7))
)

agg = (
    recent_orders
    .groupBy("channel", "customer_id")
    .agg(
        F.sum(F.when(F.col("status") == "SHIPPED", F.col("shipping_fee")).otherwise(0)).alias("shipped_shipping_fee"),
        F.min("shipping_fee").alias("min_shipping_fee"),
    )
)

result = (
    agg
    .filter(F.col("shipped_shipping_fee") < 68)
    .select("channel", "customer_id", "shipped_shipping_fee", "min_shipping_fee")
    .orderBy(F.col("shipped_shipping_fee").desc(), "channel", "customer_id")
    .limit(5)
)
