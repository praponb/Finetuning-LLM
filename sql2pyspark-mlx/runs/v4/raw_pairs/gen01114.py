from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isNull())
    .groupBy(
        F.when(F.year("start_date") < 2024, "before_2024").when(F.year("start_date") == 2024, "2024").otherwise("after_2024").alias("start_cohort"),
    )
    .agg(F.count("monthly_fee").alias("monthly_fee_count"))
    .filter(F.col("monthly_fee_count") >= 6)
    .orderBy(F.col("monthly_fee_count").desc(), "start_cohort")
)
