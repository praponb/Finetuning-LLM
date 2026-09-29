from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("start_date") < "2023-10-01") | F.month("end_date").isin([6, 10, 12]))
    .withColumn("clean_plan", F.trim("plan"))
    .withColumn("start_date_minus_1d", F.date_sub("start_date", 1))
)
