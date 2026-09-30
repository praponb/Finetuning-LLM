from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee").between(140, 210))
    .groupBy(F.month("start_date").alias("start_month"), "status")
    .agg(F.max("monthly_fee").alias("max_monthly_fee"))
    .orderBy(F.col("max_monthly_fee").desc(), F.col("start_month").desc(), "status")
    .limit(1)
)
