from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("status")
    .agg(
        F.sum(F.when(F.col("plan") == "basic", F.col("monthly_fee")).otherwise(0)).alias("basic_monthly_fee"),
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
    )
    .orderBy(F.col("basic_monthly_fee").desc(), "status")
    .limit(50)
)
