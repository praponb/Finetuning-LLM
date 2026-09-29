from pyspark.sql import functions as F

subscriptions_by_customer = (
    df_subscriptions
    .groupBy("customer_id")
    .agg(F.max("monthly_fee").alias("max_monthly_fee"))
)

result = (
    df_customers.alias("c")
    .join(subscriptions_by_customer.alias("sbc"), F.col("c.id") == F.col("sbc.customer_id"), "inner")
    .filter(F.col("sbc.max_monthly_fee") >= 230)
    .select(F.col("c.email"), F.col("c.credit_limit"), F.col("sbc.max_monthly_fee"))
    .orderBy("email")
)
