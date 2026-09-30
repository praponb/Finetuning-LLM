from pyspark.sql import functions as F

result = (
    df_subscriptions
    .agg(F.sum("monthly_fee").alias("total_monthly_fee"))
    .orderBy(F.col("total_monthly_fee").desc())
)
