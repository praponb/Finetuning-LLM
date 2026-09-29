from pyspark.sql import functions as F

valid_subscriptions = (
    df_subscriptions
    .filter(F.col("monthly_fee") < 180)
    .withColumn("clean_status", F.trim("status"))
)

agg = (
    valid_subscriptions
    .groupBy("customer_id", "plan")
    .agg(
        F.sum("monthly_fee").alias("total_monthly_fee"),
        F.round(F.stddev("monthly_fee"), 2).alias("stddev_monthly_fee"),
    )
)

result = agg.orderBy(F.col("customer_id").desc(), "total_monthly_fee")
