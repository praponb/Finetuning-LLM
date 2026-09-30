from pyspark.sql import functions as F

result = (
    df_subscriptions
    .cube(F.year("end_date").alias("end_year"), "plan")
    .agg(
        F.max("monthly_fee").alias("max_monthly_fee"),
        F.max("end_date").alias("last_end_date"),
        F.round(F.avg("monthly_fee"), 2).alias("avg_monthly_fee"),
    )
)
