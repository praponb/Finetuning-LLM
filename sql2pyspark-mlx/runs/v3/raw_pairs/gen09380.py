from pyspark.sql import functions as F

result = (
    df_subscriptions
    .withColumn("end_date_plus_months", F.add_months("end_date", 6))
    .orderBy("end_date")
)
