from pyspark.sql import functions as F

filtered_subscriptions = (
    df_subscriptions
    .filter(F.col("customer_id").isNull())
    .withColumn("status_title", F.initcap(F.trim("status")))
)

status_summary = (
    filtered_subscriptions
    .groupBy("status")
    .agg(F.min("monthly_fee").alias("min_monthly_fee"))
)

result = (
    status_summary
    .filter(F.col("min_monthly_fee") < 180)
    .select("status", "min_monthly_fee")
    .orderBy(F.col("min_monthly_fee").desc(), "status")
    .limit(5)
)
