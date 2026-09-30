from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("customer_id", F.year("start_date").alias("start_year"))
    .agg(
        F.avg("monthly_fee").alias("avg_monthly_fee"),
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
    )
    .filter(F.col("avg_monthly_fee") >= 80)
    .orderBy(F.col("avg_monthly_fee").desc(), "customer_id", "start_year")
    .limit(3)
)
