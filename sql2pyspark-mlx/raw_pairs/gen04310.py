from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(
        F.when(F.col("monthly_fee") >= 229, "PREMIUM").otherwise("STANDARD").alias("monthly_fee_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
        F.countDistinct("plan").alias("distinct_plans"),
        F.stddev_samp("monthly_fee").alias("monthly_fee_stddev"),
        F.round(F.avg("monthly_fee"), 2).alias("avg_monthly_fee"),
        F.max(F.when(F.col("status") == "EXPIRED", F.col("monthly_fee"))).alias("expired_max_monthly_fee"),
    )
    .filter(F.col("avg_monthly_fee") > 0)
)
