from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("plan") != "standard")
    .groupBy("plan")
    .agg(
        F.count("*").alias("total_count"),
        F.max("start_date").alias("last_start_date"),
        F.min("monthly_fee").alias("min_monthly_fee"),
    )
    .orderBy(F.col("min_monthly_fee").desc(), "total_count")
)
