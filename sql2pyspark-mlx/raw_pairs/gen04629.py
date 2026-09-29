from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(
        F.when(F.col("monthly_fee") >= 221, "high").when(F.col("monthly_fee") >= 108, "mid").otherwise("low").alias("monthly_fee_bucket"),
        "status",
        F.when(F.year("end_date") < 2024, "before_2024").when(F.year("end_date") == 2024, "2024").otherwise("after_2024").alias("end_cohort"),
    )
    .agg(F.max("start_date").alias("last_start_date"))
)
