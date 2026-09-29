from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "outer")
    .filter(F.col("oi.order_id").isin([8, 18, 26, 27]))
    .groupBy(
        F.when(F.col("o.amount") >= 1590, "high").when(F.col("o.amount") >= 440, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.round(F.stddev(F.col("oi.quantity")), 2).alias("stddev_quantity"),
        F.round(F.avg(F.col("oi.quantity")), 2).alias("avg_quantity"),
    )
    .filter(F.col("avg_quantity") >= 28)
)
