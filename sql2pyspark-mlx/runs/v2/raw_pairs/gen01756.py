from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(F.year("end_date").alias("end_year"))
    .agg(F.round(F.stddev("monthly_fee"), 2).alias("stddev_monthly_fee"))
    .filter(F.col("stddev_monthly_fee") >= 27)
)
