from pyspark.sql import functions as F

subscriptions_by_customer = (
    df_subscriptions
    .groupBy("customer_id")
    .agg(F.count("monthly_fee").alias("monthly_fee_count"))
)

result = (
    df_customers.alias("customers")
    .join(subscriptions_by_customer.alias("subscriptions_by_customer"), F.col("customers.id") == F.col("subscriptions_by_customer.customer_id"), "inner")
    .filter(F.col("subscriptions_by_customer.monthly_fee_count") > 7)
    .select(
        F.col("customers.id"),
        F.col("customers.city"),
        F.col("subscriptions_by_customer.monthly_fee_count"),
    )
    .orderBy(F.col("id").desc())
    .limit(1)
)
