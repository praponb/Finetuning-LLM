from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(
        F.when(F.col("monthly_fee") >= 230, "high").when(F.col("monthly_fee") >= 62, "mid").otherwise("low").alias("monthly_fee_bucket"),
        "status",
    )
    .agg(
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
        F.round(F.stddev("monthly_fee"), 2).alias("stddev_monthly_fee"),
        F.count("monthly_fee").alias("monthly_fee_count"),
    )
    .filter(F.col("monthly_fee_count") >= 1)
    .drop("monthly_fee_count")
    .orderBy("status", "monthly_fee_bucket")
    .limit(10)
)
