from pyspark.sql import functions as F

customer_summary = (
    df_subscriptions
    .filter(F.col("status") == "EXPIRED")
    .groupBy("customer_id")
    .agg(
        F.min("monthly_fee").alias("min_monthly_fee"),
        F.round(F.stddev("monthly_fee"), 2).alias("stddev_monthly_fee"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("C"), F.col("c.id") == F.col("C.customer_id"), "left")
    .select(
        F.col("c.id"),
        F.col("c.credit_limit"),
        F.col("c.city"),
        F.col("C.min_monthly_fee"),
        F.col("C.stddev_monthly_fee"),
    )
)
