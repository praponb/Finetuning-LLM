from pyspark.sql import functions as F

result = (
    df_web_events
    .select("id", "event_time", "duration_sec", F.trim("page").alias("clean_page"))
)
