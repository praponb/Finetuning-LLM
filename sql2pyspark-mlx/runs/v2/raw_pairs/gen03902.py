from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(F.date_format("end_date", "yyyy-MM").alias("end_month"), "status")
    .agg(
        F.min("monthly_fee").alias("min_monthly_fee"),
        F.sum(F.when(F.col("plan") == "standard", F.col("monthly_fee")).otherwise(0)).alias("standard_monthly_fee"),
    )
    .filter(F.col("standard_monthly_fee") < 27)
    .drop("standard_monthly_fee")
    .orderBy("min_monthly_fee")
)
