from pyspark.sql import functions as F

subscriptions_by_customer = (
    df_subscriptions
    .groupBy("customer_id")
    .agg(
        F.min("start_date").alias("min_start_date"),
        F.round(F.stddev("monthly_fee"), 2).alias("stddev_monthly_fee"),
    )
)

result = (
    df_customers.alias("c")
    .join(subscriptions_by_customer.alias("t"), F.col("c.id") == F.col("t.customer_id"), "inner")
    .select(
        F.col("c.id"),
        F.col("c.signup_date"),
        F.col("c.email"),
        F.col("t.min_start_date"),
        F.col("t.stddev_monthly_fee"),
    )
)
