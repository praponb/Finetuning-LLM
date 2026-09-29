from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("status") == "ACTIVE")
    .groupBy("status")
    .agg(
        F.count(F.when(F.col("plan") == "basic", 1)).alias("basic_count"),
        F.count("*").alias("num_subscriptions"),
        F.round(F.stddev("monthly_fee"), 2).alias("stddev_monthly_fee"),
    )
    .orderBy(F.col("stddev_monthly_fee").desc(), "status")
    .limit(20)
)
