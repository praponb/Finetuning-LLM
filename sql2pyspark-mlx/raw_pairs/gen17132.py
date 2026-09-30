from pyspark.sql import functions as F

subscriptions_by_customer = (
    df_subscriptions
    .filter(F.col("status") == "ACTIVE")
    .groupBy("customer_id")
    .agg(
        F.max("monthly_fee").alias("max_monthly_fee"),
        F.min("end_date").alias("min_end_date"),
    )
)

result = (
    df_customers.alias("c")
    .join(subscriptions_by_customer.alias("t"), F.col("c.id") == F.col("t.customer_id"), "left")
    .filter(F.col("t.max_monthly_fee") > 70)
    .select(
        F.col("c.id"),
        F.col("c.email"),
        F.col("c.signup_date"),
        F.col("t.max_monthly_fee"),
        F.col("t.min_end_date"),
    )
    .orderBy("min_end_date")
)
