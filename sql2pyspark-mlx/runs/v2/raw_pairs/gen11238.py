from pyspark.sql import functions as F

result = (
    df_subscriptions
    .cube(F.year("end_date").alias("end_year"), "plan")
    .agg(
        F.avg("monthly_fee").alias("avg_monthly_fee"),
        F.count("*").alias("row_count"),
    )
    .orderBy("plan")
)
