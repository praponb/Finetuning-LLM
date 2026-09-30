from pyspark.sql import functions as F

filtered_subscriptions = df_subscriptions.filter(F.col("end_date") < "2024-01-01")

subscriptions_stats = (
    filtered_subscriptions
    .groupBy(
        F.when(F.year("end_date") < 2024, "before_2024").when(F.year("end_date") == 2024, "2024").otherwise("after_2024").alias("end_cohort"),
    )
    .agg(F.count(F.when(F.col("status") == "ACTIVE", 1)).alias("active_count"))
)

result = subscriptions_stats.select("end_cohort", "active_count")
