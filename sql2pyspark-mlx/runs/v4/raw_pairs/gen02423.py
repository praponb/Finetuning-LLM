from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee") < 90)
    .groupBy(F.date_trunc("MONTH", "start_date").alias("start_month"))
    .agg(
        F.countDistinct("monthly_fee").alias("unique_plans"),
        F.count("*").alias("cnt"),
    )
    .orderBy(F.col("cnt").desc(), "start_month")
    .limit(1)
)
