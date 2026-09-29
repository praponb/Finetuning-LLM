from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(F.date_format("end_date", "yyyy-MM").alias("end_month"))
    .agg(
        F.max("start_date").alias("last_start_date"),
        F.count(F.when(F.col("plan") == "enterprise", 1)).alias("enterprise_count"),
        F.sum("monthly_fee").alias("total_monthly_fee"),
        F.count(F.when(F.col("plan") == "standard", 1)).alias("standard_count"),
    )
    .filter(F.col("standard_count") > 3)
    .drop("standard_count")
    .orderBy("enterprise_count")
)
