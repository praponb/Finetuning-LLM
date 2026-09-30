from pyspark.sql import functions as F

filtered_web_events = (
    df_web_events
    .filter(F.col("session_id").isin([13, 24, 39]))
    .withColumn("event_time_hour", F.hour("event_time"))
)

page_summary = (
    filtered_web_events
    .groupBy("page", F.hour("event_time").alias("event_hour"))
    .agg(F.max("event_time").alias("last_event_time"))
)

result = (
    page_summary
    .select("page", "event_hour", "last_event_time")
    .orderBy("last_event_time", F.col("event_hour").desc(), "page")
    .limit(50)
)
