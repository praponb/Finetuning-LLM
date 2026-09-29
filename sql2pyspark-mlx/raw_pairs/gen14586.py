from pyspark.sql import functions as F

src = (
    df_subscriptions
    .filter((F.col("status") != "CANCELLED") & (F.col("plan") == "premium"))
)

end_month_summary = (
    src
    .groupBy(F.date_format("end_date", "yyyy-MM").alias("end_month"))
    .agg(F.round(F.stddev("monthly_fee"), 2).alias("stddev_monthly_fee"))
)

result = (
    end_month_summary
    .filter(F.col("stddev_monthly_fee") < 77)
    .select("end_month", "stddev_monthly_fee")
    .orderBy("end_month")
)
