from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "left")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .filter(F.col("o.customer_id").isin([4, 35]))
    .groupBy(F.col("c.country"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("o.status") == "SHIPPED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_shipped"),
        F.round(F.stddev(F.col("o.shipping_fee")), 2).alias("stddev_shipping_fee"),
    )
    .filter(F.col("pct_shipped") > 20)
)
