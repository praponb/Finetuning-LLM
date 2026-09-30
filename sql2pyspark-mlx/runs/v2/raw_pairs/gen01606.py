from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(F.quarter("end_date").alias("end_quarter"))
    .agg(F.countDistinct("monthly_fee").alias("unique_plans"))
    .filter(F.col("unique_plans") > 7)
    .orderBy(F.col("end_quarter").desc())
    .limit(10)
)
