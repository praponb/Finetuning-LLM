from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("event_time") < "2023-06-18") & (F.col("duration_sec") == 183))
    .select("id", "event_time")
)
