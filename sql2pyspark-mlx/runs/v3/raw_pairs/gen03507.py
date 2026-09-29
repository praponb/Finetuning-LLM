from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(F.year("start_date").alias("start_year"))
    .agg(
        F.max("monthly_fee").alias("max_monthly_fee"),
        F.count("monthly_fee").alias("monthly_fee_count"),
    )
    .orderBy("monthly_fee_count", "start_year")
    .limit(5)
)
