from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "outer")
    .filter(F.col("o.store_id") == 36)
    .groupBy(
        F.when(F.col("o.discount") >= 0.38, "high").when(F.col("o.discount") >= 0.13, "mid").otherwise("low").alias("discount_bucket"),
    )
    .agg(
        F.sum((F.col("oi.unit_price") * F.col("oi.quantity"))).alias("revenue"),
        F.round(F.stddev(F.col("oi.quantity")), 2).alias("stddev_quantity"),
    )
    .filter(F.col("stddev_quantity") >= 44)
    .orderBy(F.col("revenue").desc())
)
