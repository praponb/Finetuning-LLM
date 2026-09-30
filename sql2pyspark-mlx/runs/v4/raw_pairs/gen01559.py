from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(
        F.when(F.col("monthly_fee") >= 198, "HIGH").otherwise("LOW").alias("monthly_fee_tier"),
    )
    .agg(
        F.count_if(F.col("status") == "CANCELLED").alias("cancelled_events"),
        F.round(F.avg("monthly_fee"), 2).alias("avg_monthly_fee"),
        F.round(F.avg("monthly_fee"), 2).alias("avg_monthly_fee_2"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.size(F.collect_set("plan")).alias("num_plans"),
    )
    .orderBy(F.col("num_plans").desc())
)
