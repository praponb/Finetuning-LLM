from pyspark.sql import functions as F

subscriptions_by_customer = (
    df_subscriptions
    .groupBy("customer_id")
    .agg(F.round(F.avg("monthly_fee"), 2).alias("avg_monthly_fee"))
)

result = (
    df_customers.alias("c")
    .join(subscriptions_by_customer.alias("t"), F.col("c.id") == F.col("t.customer_id"), "inner")
    .filter(F.col("t.avg_monthly_fee") >= 28)
    .select(F.col("c.signup_date"), F.col("t.avg_monthly_fee"))
    .orderBy(F.col("signup_date").desc())
)
