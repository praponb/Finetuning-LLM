from pyspark.sql import functions as F

recent_subscriptions = (
    df_subscriptions
    .filter((F.col("start_date") >= "2023-11-01") & (F.col("status") != "EXPIRED"))
)

summary = (
    recent_subscriptions
    .groupBy("status", "plan")
    .agg(F.min("end_date").alias("first_end_date"))
)

result = (
    summary
    .orderBy("plan", "status")
    .limit(3)
)
