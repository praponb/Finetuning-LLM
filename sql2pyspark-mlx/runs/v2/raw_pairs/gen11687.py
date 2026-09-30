from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(
        F.date_trunc("MONTH", "end_date").alias("end_month"),
        F.year("end_date").alias("end_year"),
    )
    .agg(F.max("monthly_fee").alias("max_monthly_fee"))
)
