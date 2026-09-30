from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee").between(160, 170))
    .groupBy("plan")
    .agg(
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
        (F.max("monthly_fee") - F.min("monthly_fee")).alias("monthly_fee_range"),
        F.min("monthly_fee").alias("min_monthly_fee"),
    )
    .orderBy(F.col("monthly_fee_range").desc(), "plan")
)
