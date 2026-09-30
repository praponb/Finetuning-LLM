from pyspark.sql import functions as F

customer_stats = (
    df_orders
    .groupBy("customer_id")
    .agg(F.round(F.stddev("shipping_fee"), 2).alias("stddev_shipping_fee"))
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "left")
    .filter(F.col("cs.stddev_shipping_fee") > 79)
    .select(
        F.col("c.id"),
        F.col("c.credit_limit"),
        F.col("c.city"),
        F.col("cs.stddev_shipping_fee"),
    )
)
