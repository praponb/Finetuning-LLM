from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("start_date").between("2024-01-17", "2024-07-08"))
    .groupBy("status")
    .agg(F.min("monthly_fee").alias("min_monthly_fee"))
    .filter(F.col("min_monthly_fee") > 200)
    .orderBy(F.col("status").desc())
)
