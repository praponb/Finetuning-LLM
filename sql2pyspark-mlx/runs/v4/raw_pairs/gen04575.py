from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(
        "plan",
        F.when(F.year("end_date") < 2024, "before_2024").when(F.year("end_date") == 2024, "2024").otherwise("after_2024").alias("end_cohort"),
    )
    .agg(
        F.count("*").alias("cnt"),
        (F.max("monthly_fee") - F.min("monthly_fee")).alias("monthly_fee_range"),
    )
    .filter(F.col("cnt") < 10)
    .orderBy("monthly_fee_range", "plan", "end_cohort")
    .limit(20)
)
