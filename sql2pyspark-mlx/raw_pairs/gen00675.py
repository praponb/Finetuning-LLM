from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("status") != "CANCELLED") | F.col("monthly_fee").isNotNull())
    .withColumn("monthly_fee_whole", F.ceil("monthly_fee"))
    .withColumn("days_start_date_to_end_date", F.datediff("end_date", "start_date"))
)
