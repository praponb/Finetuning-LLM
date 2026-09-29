from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull() & (F.col("monthly_fee").between(70, 130) | (F.col("end_date") >= "2024-05-01")))
    .groupBy(
        "plan",
        F.date_trunc("MONTH", "end_date").alias("end_month"),
        F.when(F.year("end_date") < 2024, "before_2024").when(F.year("end_date") == 2024, "2024").otherwise("after_2024").alias("end_cohort"),
    )
    .agg(
        F.round(F.avg("monthly_fee"), 2).alias("avg_monthly_fee"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("cnt") < 3)
)
