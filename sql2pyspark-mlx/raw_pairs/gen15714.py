from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("status")
    .agg(F.min("monthly_fee").alias("min_monthly_fee"))
    .orderBy(F.col("min_monthly_fee").desc())
)
