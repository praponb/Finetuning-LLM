from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(
        F.when(F.year("end_date") < 2024, "before_2024").when(F.year("end_date") == 2024, "2024").otherwise("after_2024").alias("end_cohort"),
    )
    .agg(
        F.countDistinct("monthly_fee").alias("unique_statuss"),
        F.sum(F.when(F.col("plan") == "basic", 1).otherwise(0)).alias("basic_count"),
    )
    .filter(F.col("unique_statuss") < 8)
    .select("end_cohort", "unique_statuss", "basic_count")
    .orderBy("end_cohort")
    .limit(10)
)
