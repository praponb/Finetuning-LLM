from pyspark.sql import functions as F

result = (
    df_web_events
    .select("event_type")
    .distinct()
    .orderBy("event_type")
)
