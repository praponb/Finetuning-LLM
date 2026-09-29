from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("monthly_fee") <= 140) & (F.col("plan") != "premium"))
    .withColumn("weeks_start_date_to_end_date", (F.datediff("end_date", "start_date") / 7))
    .orderBy("weeks_start_date_to_end_date", "id")
    .limit(3)
)
