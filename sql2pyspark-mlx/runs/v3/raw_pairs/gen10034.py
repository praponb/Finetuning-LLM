from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .groupBy(F.date_format(F.col("c.signup_date"), "yyyy-MM").alias("signup_month"))
    .agg(
        F.sum(F.when(F.col("s.plan") == "enterprise", F.col("s.monthly_fee")).otherwise(0)).alias("enterprise_monthly_fee"),
        F.round(F.stddev(F.col("s.monthly_fee")), 2).alias("stddev_monthly_fee"),
    )
    .orderBy(F.col("stddev_monthly_fee").desc(), "signup_month")
    .limit(1)
)
