from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(F.month("start_date").alias("start_month"))
    .agg(
        F.max("monthly_fee").alias("max_monthly_fee"),
        F.round(F.stddev("monthly_fee"), 2).alias("stddev_monthly_fee"),
        F.count(F.when(F.col("plan") == "standard", 1)).alias("standard_count"),
    )
    .filter(F.col("standard_count") >= 5)
    .drop("standard_count")
    .orderBy(F.col("start_month").desc())
    .limit(50)
)
