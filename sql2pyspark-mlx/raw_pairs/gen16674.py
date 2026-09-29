from pyspark.sql import functions as F

subscriptions_by_customer = (
    df_subscriptions
    .filter(F.col("plan") == "standard")
    .groupBy("customer_id")
    .agg(
        F.avg("monthly_fee").alias("avg_monthly_fee"),
        F.max("start_date").alias("latest_start_date"),
    )
)

result = (
    df_customers.alias("c")
    .join(subscriptions_by_customer.alias("sbc"), F.col("c.id") == F.col("sbc.customer_id"), "left")
    .select(
        F.col("c.id"),
        F.col("c.signup_date"),
        F.col("c.credit_limit"),
        F.col("sbc.avg_monthly_fee"),
        F.col("sbc.latest_start_date"),
    )
)
