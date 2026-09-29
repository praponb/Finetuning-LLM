from pyspark.sql import functions as F

customer_summary = (
    df_subscriptions
    .filter(F.col("customer_id") == 34)
    .groupBy("customer_id")
    .agg(F.round(F.stddev("monthly_fee"), 2).alias("stddev_monthly_fee"))
)

result = (
    df_customers.alias("customers")
    .join(customer_summary.alias("t"), F.col("customers.id") == F.col("t.customer_id"), "inner")
    .select(
        F.col("customers.id"),
        F.col("customers.is_active"),
        F.col("customers.email"),
        F.col("t.stddev_monthly_fee"),
    )
    .orderBy(F.col("stddev_monthly_fee").desc())
)
