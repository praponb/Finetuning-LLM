from pyspark.sql import functions as F

valid_subscriptions = (
    df_subscriptions
    .filter(F.col("customer_id").isin([8, 14, 30]))
    .withColumn("plan_title", F.initcap(F.trim("plan")))
)

agg = (
    valid_subscriptions
    .groupBy(F.year("end_date").alias("end_year"))
    .agg(
        F.countDistinct("monthly_fee").alias("unique_plans"),
        F.sum(F.when(F.col("plan") == "premium", F.col("monthly_fee")).otherwise(0)).alias("premium_monthly_fee"),
    )
)

result = (
    agg
    .filter(F.col("unique_plans") >= 5)
    .orderBy(F.col("premium_monthly_fee").desc())
)
