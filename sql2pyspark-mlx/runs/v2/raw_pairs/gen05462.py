from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "outer")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .filter(F.col("o.customer_id").isNotNull())
    .groupBy(F.month(F.col("c.signup_date")).alias("signup_month"))
    .agg(
        F.avg(F.col("o.shipping_fee")).alias("avg_shipping_fee"),
        F.round(F.stddev(F.col("o.amount")), 2).alias("stddev_amount"),
    )
    .filter(F.col("avg_shipping_fee") > 35)
    .orderBy(F.col("signup_month").desc())
    .limit(10)
)
