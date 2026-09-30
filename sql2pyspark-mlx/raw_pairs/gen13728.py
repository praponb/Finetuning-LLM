from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_type") == "add_to_cart")
    .withColumn("event_time_date", F.to_date("event_time"))
    .withColumn("event_time_date_2", F.to_date("event_time"))
)
