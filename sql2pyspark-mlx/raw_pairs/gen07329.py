from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("plan")
    .agg(F.max("start_date").alias("latest_start_date"))
    .orderBy("plan")
)
