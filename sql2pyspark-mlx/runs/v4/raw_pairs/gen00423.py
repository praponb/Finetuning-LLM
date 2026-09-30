from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(F.date_format("end_date", "yyyy-MM").alias("end_month"), "status")
    .agg(
        F.sum("monthly_fee").alias("total_monthly_fee"),
        (F.max("monthly_fee") - F.min("monthly_fee")).alias("monthly_fee_range"),
    )
    .orderBy(F.col("total_monthly_fee").desc(), "end_month", "status")
    .limit(20)
)
