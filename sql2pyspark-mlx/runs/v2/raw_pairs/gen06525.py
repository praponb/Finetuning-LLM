from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(F.month("end_date").alias("end_month"))
    .agg(
        (F.max("monthly_fee") - F.min("monthly_fee")).alias("monthly_fee_range"),
        F.sum("monthly_fee").alias("total_monthly_fee"),
    )
    .filter(F.col("total_monthly_fee") > 740)
    .select("end_month", "monthly_fee_range", "total_monthly_fee")
    .orderBy(F.col("total_monthly_fee").desc())
)
