from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(F.quarter("start_date").alias("start_quarter"))
    .agg(F.min("start_date").alias("first_start_date"))
    .orderBy("first_start_date")
)
