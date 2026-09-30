from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .groupBy(
        F.month(F.col("c.signup_date")).alias("signup_month"),
        F.col("c.is_active"),
    )
    .agg(F.round(F.stddev(F.col("s.monthly_fee")), 2).alias("stddev_monthly_fee"))
    .orderBy(F.col("signup_month").desc())
)
