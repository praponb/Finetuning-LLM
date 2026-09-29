from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "outer")
    .groupBy(F.col("s.city"))
    .agg(
        F.min(F.col("o.order_date")).alias("min_order_date"),
        F.round(F.stddev(F.col("o.shipping_fee")), 2).alias("stddev_shipping_fee"),
    )
    .filter(F.col("stddev_shipping_fee") > 47)
    .orderBy("city")
    .limit(50)
)
