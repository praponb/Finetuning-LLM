from pyspark.sql import functions as F

valid_subscriptions = (
    df_subscriptions
    .filter(F.col("plan") == "standard")
    .withColumn("monthly_fee_tier", F.when(F.col("monthly_fee") >= 241, "High").when(F.col("monthly_fee") >= 80, "Medium").otherwise("Low"))
)

summary = (
    valid_subscriptions
    .groupBy("status")
    .agg(F.max("end_date").alias("latest_end_date"))
)

result = (
    summary
    .select("status", "latest_end_date")
    .orderBy("status")
    .limit(5)
)
