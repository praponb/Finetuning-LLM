from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([12, 27]))
    .select("session_id", "page", F.to_date("event_time").alias("event_time_date"))
)
