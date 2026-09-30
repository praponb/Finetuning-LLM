from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.month("start_date").isin([2, 8, 12]))
    .withColumn("days_end_date_to_start_date", F.datediff("start_date", "end_date"))
)
