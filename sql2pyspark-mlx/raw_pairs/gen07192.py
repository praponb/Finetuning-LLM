from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee") < 160)
    .groupBy(F.month("start_date").alias("start_month"), "status")
    .agg(
        F.count(F.when(F.col("status") == "ACTIVE", 1)).alias("active_count"),
        F.max("monthly_fee").alias("max_monthly_fee"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("active_count") > 5)
    .orderBy(F.col("max_monthly_fee").desc(), "start_month", "status")
    .limit(5)
)
