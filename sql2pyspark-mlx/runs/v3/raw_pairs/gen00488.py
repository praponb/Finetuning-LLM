from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "left")
    .filter(F.col("o.channel").isin(["web", "mobile"]))
    .groupBy(
        F.when(F.col("o.discount") >= 0.41, "high").when(F.col("o.discount") >= 0.11, "mid").otherwise("low").alias("discount_bucket"),
    )
    .agg(
        F.round(F.stddev(F.col("oi.quantity")), 2).alias("stddev_quantity"),
        F.count(F.col("oi.quantity")).alias("quantity_count"),
    )
    .orderBy("discount_bucket", F.col("quantity_count").desc())
)
