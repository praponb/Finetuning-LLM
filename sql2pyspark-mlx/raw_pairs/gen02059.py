from pyspark.sql import functions as F

filtered_subscriptions = df_subscriptions.filter(F.col("customer_id").isin([9, 35]))

subscriptions_stats = (
    filtered_subscriptions
    .groupBy(
        F.when(F.year("start_date") < 2024, "before_2024").when(F.year("start_date") == 2024, "2024").otherwise("after_2024").alias("start_cohort"),
    )
    .agg(F.count("monthly_fee").alias("monthly_fee_count"))
)

result = (
    subscriptions_stats
    .filter(F.col("monthly_fee_count") > 6)
    .select("start_cohort", "monthly_fee_count")
    .orderBy("start_cohort")
)
