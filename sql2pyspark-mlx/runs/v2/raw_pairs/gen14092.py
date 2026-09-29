from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("subscriptions")
    .join(df_customers.alias("customers"), F.col("customers.id") == F.col("subscriptions.customer_id"), "inner")
    .filter(F.col("subscriptions.monthly_fee") > 130)
    .groupBy(F.col("customers.city"))
    .agg(
        F.round(F.stddev(F.col("subscriptions.monthly_fee")), 2).alias("stddev_monthly_fee"),
    )
)
