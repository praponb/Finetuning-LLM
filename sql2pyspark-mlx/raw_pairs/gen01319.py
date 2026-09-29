from pyspark.sql import functions as F

result = (
    df_subscriptions
    .withColumn("end_date_month", F.month("end_date"))
    .withColumn("start_date_year", F.year("start_date"))
    .orderBy("end_date")
)
