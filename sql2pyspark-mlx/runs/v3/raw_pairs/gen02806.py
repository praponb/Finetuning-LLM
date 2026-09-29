from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull())
    .groupBy(
        F.date_format("start_date", "yyyy-MM").alias("start_month"),
        "status",
        F.when(F.year("start_date") < 2024, "before_2024").when(F.year("start_date") == 2024, "2024").otherwise("after_2024").alias("start_cohort"),
    )
    .agg(
        F.max("start_date").alias("latest_start_date"),
        (F.max("monthly_fee") - F.min("monthly_fee")).alias("monthly_fee_range"),
    )
    .filter(F.col("monthly_fee_range") < 65)
    .orderBy("start_month", "status", "start_cohort")
    .limit(10)
)
