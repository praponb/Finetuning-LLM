from pyspark.sql import functions as F

filtered_subscriptions = (
    df_subscriptions
    .filter((F.col("monthly_fee") >= 190) & F.col("status").isin(["CANCELLED", "EXPIRED"]) & (F.col("start_date") >= "2023-06-30"))
)

agg = (
    filtered_subscriptions
    .groupBy(F.year("end_date").alias("end_year"))
    .agg(
        F.max("monthly_fee").alias("max_monthly_fee"),
        F.avg("monthly_fee").alias("avg_monthly_fee"),
    )
)

result = agg.filter(F.col("avg_monthly_fee") >= 180)
