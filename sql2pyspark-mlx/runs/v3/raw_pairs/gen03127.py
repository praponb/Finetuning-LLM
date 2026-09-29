from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee") > 180)
    .groupBy("plan", "customer_id")
    .agg(
        F.min("start_date").alias("first_start_date"),
        F.round(F.stddev("monthly_fee"), 2).alias("stddev_monthly_fee"),
    )
)
