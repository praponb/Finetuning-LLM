from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_type") == "purchase")
    .select("duration_sec", F.to_date("event_time").alias("event_time_date"))
)
