from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("status").isin(["CANCELLED", "PAUSED", "EXPIRED"]))
    .groupBy(
        F.when(F.col("monthly_fee") >= 183, "high").when(F.col("monthly_fee") >= 104, "mid").otherwise("low").alias("monthly_fee_bucket"),
    )
    .agg(
        F.avg("monthly_fee").alias("avg_monthly_fee"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
    )
    .filter(F.col("avg_monthly_fee") > 170)
)
