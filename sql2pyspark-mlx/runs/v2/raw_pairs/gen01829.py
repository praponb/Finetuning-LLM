from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.month("start_date").isin([5, 7, 10]) & (F.col("end_date") > "2023-06-01"))
    .withColumn("plan_prefix", F.substring("plan", 1, 3))
)
