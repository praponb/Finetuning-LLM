from pyspark.sql import functions as F

customer_stats = (
    df_subscriptions
    .groupBy("customer_id")
    .agg(F.round(F.stddev("monthly_fee"), 2).alias("stddev_monthly_fee"))
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("C"), F.col("c.id") == F.col("C.customer_id"), "inner")
    .filter(F.col("C.stddev_monthly_fee") > 39)
    .select(F.col("c.id"), F.col("c.is_active"), F.col("C.stddev_monthly_fee"))
    .orderBy(F.col("stddev_monthly_fee").desc())
)
