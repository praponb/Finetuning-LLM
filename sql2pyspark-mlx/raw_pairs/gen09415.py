from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_orders.alias("o"), F.col("c.id") == F.col("o.customer_id"), "inner")
    .filter(F.col("o.discount").between(0.31, 0.35))
    .groupBy(F.col("c.id"), F.col("c.city"))
    .agg(
        F.count(F.col("o.id")).alias("order_count"),
        F.round(F.stddev(F.col("o.shipping_fee")), 2).alias("stddev_shipping_fee"),
        F.max(F.col("o.discount")).alias("max_discount"),
    )
    .filter(F.col("max_discount") > 0.25)
    .orderBy("order_count")
)
