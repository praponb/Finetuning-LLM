from pyspark.sql import functions as F

recent_subscriptions = (
    df_subscriptions
    .filter(F.col("status") == "CANCELLED")
    .withColumn("end_date_weekofyear", F.weekofyear("end_date"))
)

summary = (
    recent_subscriptions
    .groupBy("plan")
    .agg(F.count("monthly_fee").alias("monthly_fee_count"))
)

result = summary
