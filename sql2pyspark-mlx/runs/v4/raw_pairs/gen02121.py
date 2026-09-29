from pyspark.sql import functions as F

valid_subscriptions = (
    df_subscriptions
    .filter(F.col("status").isin(["CANCELLED", "PAUSED", "EXPIRED"]) & (F.col("start_date") > "2023-08-24") & (F.col("monthly_fee") <= 230))
)

agg = (
    valid_subscriptions
    .groupBy(
        F.when(F.year("start_date") < 2024, "before_2024").when(F.year("start_date") == 2024, "2024").otherwise("after_2024").alias("start_cohort"),
        F.year("start_date").alias("start_year"),
    )
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.count(F.when(F.col("plan") == "premium", 1)).alias("premium_count"),
    )
)

result = (
    agg
    .select("start_cohort", "start_year", "status_list", "premium_count")
    .orderBy("start_cohort")
)
