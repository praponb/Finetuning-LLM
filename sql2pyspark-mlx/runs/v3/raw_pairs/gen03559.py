from pyspark.sql import functions as F

base = (
    df_subscriptions
    .filter(((F.col("end_date") > "2025-03-01") & F.col("customer_id").isNull()) | (F.col("monthly_fee") > 140))
    .withColumn("start_date_day", F.dayofmonth("start_date"))
)

end_cohort_summary = (
    base
    .groupBy(
        F.when(F.year("end_date") < 2024, "before_2024").when(F.year("end_date") == 2024, "2024").otherwise("after_2024").alias("end_cohort"),
        "customer_id",
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
)

result = end_cohort_summary.select("end_cohort", "customer_id")
