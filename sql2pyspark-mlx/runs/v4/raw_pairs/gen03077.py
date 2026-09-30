from pyspark.sql import functions as F

result = (
    df_web_events
    .withColumn("event_time_date", F.to_date("event_time"))
    .withColumn("duration_sec_filled", F.coalesce("duration_sec", F.lit(0)))
)
