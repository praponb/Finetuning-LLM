from pyspark.sql import functions as F

customer_summary = (
    df_subscriptions
    .groupBy("customer_id")
    .agg(F.min("monthly_fee").alias("min_monthly_fee"))
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "left")
    .select(F.col("c.id"), F.col("c.signup_date"), F.col("cs.min_monthly_fee"))
    .orderBy(F.col("min_monthly_fee").desc())
)
