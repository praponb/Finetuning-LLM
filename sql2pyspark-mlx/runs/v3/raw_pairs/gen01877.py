from pyspark.sql import functions as F

result = (
    df_subscriptions
    .withColumn("end_date_weekofyear", F.weekofyear("end_date"))
    .withColumn("start_date_month_end", F.last_day("start_date"))
)
