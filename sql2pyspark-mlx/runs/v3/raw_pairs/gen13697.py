from pyspark.sql import functions as F

result = (
    df_subscriptions
    .agg(F.max("monthly_fee").alias("max_monthly_fee"))
    .orderBy(F.col("max_monthly_fee").desc())
)
