from pyspark.sql import functions as F

subscriptions_by_customer = (
    df_subscriptions
    .groupBy("customer_id")
    .agg(F.min("monthly_fee").alias("min_monthly_fee"))
)

result = (
    df_customers.alias("c")
    .join(subscriptions_by_customer.alias("sbc"), F.col("c.id") == F.col("sbc.customer_id"), "left")
    .filter(F.col("sbc.min_monthly_fee") >= 220)
    .select(
        F.col("c.id"),
        F.col("c.email"),
        F.col("c.name"),
        F.col("sbc.min_monthly_fee"),
    )
)
