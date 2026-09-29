from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee") > 100)
    .withColumn("days_end_date_to_start_date", F.datediff("start_date", "end_date"))
    .withColumn("weeks_end_date_to_start_date", (F.datediff("start_date", "end_date") / 7))
)
