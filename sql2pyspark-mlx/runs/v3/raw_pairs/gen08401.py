from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(F.quarter("end_date").alias("end_quarter"))
    .agg(F.max("monthly_fee").alias("max_monthly_fee"))
    .filter(F.col("max_monthly_fee") < 120)
    .orderBy(F.col("max_monthly_fee").desc(), "end_quarter")
    .limit(10)
)
