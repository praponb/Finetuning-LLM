from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(
        F.when(F.year("start_date") < 2024, "before_2024").when(F.year("start_date") == 2024, "2024").otherwise("after_2024").alias("start_cohort"),
        "status",
    )
    .agg(
        F.max("end_date").alias("last_end_date"),
        F.max("monthly_fee").alias("max_monthly_fee"),
    )
)
