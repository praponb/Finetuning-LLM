from pyspark.sql import functions as F

src = (
    df_subscriptions
    .filter(F.col("status") != "EXPIRED")
    .withColumn("end_date_minus_29d", F.date_sub("end_date", 29))
)

summary = (
    src
    .groupBy("plan")
    .agg(
        F.max("start_date").alias("latest_start_date"),
        F.sum(F.when(F.col("plan") == "enterprise", F.col("monthly_fee")).otherwise(0)).alias("enterprise_monthly_fee"),
    )
)

result = summary
