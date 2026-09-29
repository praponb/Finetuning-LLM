from pyspark.sql import functions as F

subscriptions_by_customer = (
    df_subscriptions
    .groupBy("customer_id")
    .agg(F.max("monthly_fee").alias("max_monthly_fee"))
)

result = (
    df_customers.alias("customers")
    .join(subscriptions_by_customer.alias("subscriptions_by_customer"), F.col("customers.id") == F.col("subscriptions_by_customer.customer_id"), "left")
    .filter(F.col("subscriptions_by_customer.max_monthly_fee") >= 230)
    .select(
        F.col("customers.id"),
        F.col("customers.country"),
        F.col("customers.is_active"),
        F.col("subscriptions_by_customer.max_monthly_fee"),
    )
    .orderBy(F.col("max_monthly_fee").desc())
)
