from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee").between(220, 230))
    .withColumn("plan_prefix", F.substring("plan", 1, 4))
    .withColumn("start_date_period", F.date_trunc("week", "start_date"))
)
