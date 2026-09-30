from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee") <= 240)
    .withColumn("start_date_month_end", F.last_day("start_date"))
    .withColumn("end_date_year", F.year("end_date"))
)
