from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("subscriptions")
    .join(df_customers.alias("customers"), F.col("customers.id") == F.col("subscriptions.customer_id"), "inner")
    .filter(F.length(F.col("customers.name")) > 8)
    .groupBy(
        F.year(F.col("customers.signup_date")).alias("signup_year"),
        F.col("customers.country"),
    )
    .agg(
        F.round(F.stddev(F.col("subscriptions.monthly_fee")), 2).alias("stddev_monthly_fee"),
    )
    .orderBy("stddev_monthly_fee", "country")
)
