from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee").isNull())
    .groupBy(
        F.when(F.year("end_date") < 2024, "before_2024").when(F.year("end_date") == 2024, "2024").otherwise("after_2024").alias("end_cohort"),
    )
    .agg(
        F.sum(F.when(F.col("status") == "CANCELLED", 1).otherwise(0)).alias("cancelled_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set("plan"))).alias("plan_values"),
    )
    .orderBy(F.col("end_cohort").desc())
    .limit(50)
)
