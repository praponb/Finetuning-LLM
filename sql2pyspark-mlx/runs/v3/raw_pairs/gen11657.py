from pyspark.sql import functions as F

filtered_subscriptions = (
    df_subscriptions
    .filter(((F.col("status") == "ACTIVE") & (F.col("monthly_fee") >= 80)) | (F.col("start_date") <= "2024-08-14"))
)

end_month_summary = (
    filtered_subscriptions
    .groupBy(F.date_trunc("MONTH", "end_date").alias("end_month"))
    .agg(
        F.sum(F.when(F.col("status") == "PAUSED", F.col("monthly_fee")).otherwise(0)).alias("paused_monthly_fee"),
    )
)

result = (
    end_month_summary
    .filter(F.col("paused_monthly_fee") < 75)
    .orderBy("end_month")
)
