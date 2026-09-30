from pyspark.sql import functions as F

result = (
    df_subscriptions
    .withColumn("start_date_day", F.dayofmonth("start_date"))
    .orderBy("customer_id", F.col("id").desc())
)
