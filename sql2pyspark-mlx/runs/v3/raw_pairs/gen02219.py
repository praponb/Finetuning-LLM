from pyspark.sql import functions as F

subscriptions_by_customer = (
    df_subscriptions
    .groupBy("customer_id")
    .agg(
        F.sum(F.when(F.col("plan") == "premium", F.col("monthly_fee")).otherwise(0)).alias("premium_monthly_fee"),
    )
)

result = (
    df_customers.alias("customers")
    .join(subscriptions_by_customer.alias("subscriptions_by_customer"), F.col("customers.id") == F.col("subscriptions_by_customer.customer_id"), "inner")
    .select(
        F.col("customers.id"),
        F.col("customers.country"),
        F.col("subscriptions_by_customer.premium_monthly_fee"),
    )
    .orderBy("id")
    .limit(5)
)
